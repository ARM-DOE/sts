package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/log"
)

// GetClientStatus to fulfill sts.ClientManager interface
func (c *Client) GetClientStatus(
	clientID,
	clientName,
	clientOS string,
) (status sts.ClientStatus, err error) {
	if err = c.init(); err != nil {
		return
	}
	var req *http.Request
	var resp *http.Response
	var reader io.ReadCloser
	url := fmt.Sprintf("%s/client/%s/status", c.rootURL(), clientID)
	log.Debug("GET", url, "...")
	if req, err = http.NewRequest("GET", url, bytes.NewReader([]byte(""))); err != nil {
		return
	}
	q := req.URL.Query()
	q.Add("name", clientName)
	q.Add("os", clientOS)
	req.URL.RawQuery = q.Encode()
	if resp, err = c.client.Do(req); err != nil {
		return
	}
	if reader, err = GetRespReader(resp); err != nil {
		return
	}
	defer reader.Close()
	statusBytes, err := io.ReadAll(reader)
	if err != nil {
		return
	}
	status8, err := strconv.ParseUint(string(statusBytes), 10, 8)
	if err != nil {
		err = fmt.Errorf(
			"failed to parse status: %s (server response: %s)", err, string(statusBytes),
		)
	}
	status = sts.ClientStatus(status8)
	return
}

// GetClientConf to fulfill sts.ClientManager interface
func (c *Client) GetClientConf(clientID string) (conf *sts.ClientConf, err error) {
	if err = c.init(); err != nil {
		return
	}
	var req *http.Request
	var resp *http.Response
	var reader io.ReadCloser
	url := fmt.Sprintf("%s/client/%s/conf", c.rootURL(), clientID)
	log.Debug("GET", url, "...")
	if req, err = http.NewRequest("GET", url, bytes.NewReader([]byte(""))); err != nil {
		return
	}
	if resp, err = c.client.Do(req); err != nil {
		return
	}
	if reader, err = GetRespReader(resp); err != nil {
		return
	}
	defer reader.Close()
	confBytes, err := io.ReadAll(reader)
	if err != nil {
		return
	}
	conf = &sts.ClientConf{}
	err = json.Unmarshal(confBytes, conf)
	if err != nil {
		err = fmt.Errorf(
			"failed to parse client conf: %s (server response: %s)", err, string(confBytes),
		)
	}
	return
}

// SetClientConfReceived to fulfill sts.ClientManager interface
func (c *Client) SetClientConfReceived(clientID string, when time.Time) error {
	// N/A currently
	return nil
}

// SetClientState to fulfill sts.ClientManager interface
func (c *Client) SetClientState(clientID string, state sts.ClientState) (err error) {
	if err = c.init(); err != nil {
		return
	}
	jsonBytes, err := json.Marshal(state)
	if err != nil {
		return
	}
	var req *http.Request
	var resp *http.Response
	url := fmt.Sprintf("%s/client/%s/state", c.rootURL(), clientID)
	log.Debug("PUT", url, "...")
	if req, err = http.NewRequest("PUT", url, bytes.NewReader(jsonBytes)); err != nil {
		return
	}
	if resp, err = c.client.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return
	default:
		err = fmt.Errorf(
			"set state request failed with response code: %d",
			resp.StatusCode)
	}
	return
}

// routeClientManagement handles the HTTP routes for getting a client's status
// and configuration
func (s *Server) routeClientManagement(w http.ResponseWriter, r *http.Request) {
	log.Debug("Handling:", r.URL.RequestURI())
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 || s.ClientManager == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	action := parts[len(parts)-1]
	clientID := parts[len(parts)-2]
	switch r.Method {
	case http.MethodGet:
		switch action {
		case "status":
			clientName := r.URL.Query()["name"][0]
			clientOS := r.URL.Query()["os"][0]
			status, err := s.ClientManager.GetClientStatus(
				clientID, clientName, clientOS,
			)
			if err != nil {
				handleError(w, http.StatusInternalServerError, err)
				return
			}
			log.Debug("Client Status:", clientID, clientName, clientOS, ":", status)
			err = s.respond(
				w,
				http.StatusOK,
				[]byte(strconv.FormatUint(uint64(status), 10)))
			if err != nil {
				log.Error(err.Error())
			}
			return
		case "conf":
			config, err := s.ClientManager.GetClientConf(clientID)
			if err != nil {
				handleError(w, http.StatusInternalServerError, err)
				return
			}
			var respJSON []byte
			respJSON, err = json.Marshal(config)
			if err != nil {
				handleError(w, http.StatusInternalServerError, err)
				return
			}
			log.Debug("Client Conf:", string(respJSON))
			w.Header().Set(HeaderContentType, HeaderJSON)
			if err = s.respond(w, http.StatusOK, respJSON); err != nil {
				log.Error(err.Error())
			}
			_ = s.ClientManager.SetClientConfReceived(clientID, time.Now())
			return
		}
	case http.MethodPut:
		switch action {
		case "state":
			state := sts.ClientState{}
			var br io.Reader
			var err error
			if br, err = GetReqReader(r); err != nil {
				log.Error(err.Error())
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			decoder := json.NewDecoder(br)
			if err = decoder.Decode(&state); err != nil {
				log.Error(err.Error())
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if err = s.ClientManager.SetClientState(clientID, state); err != nil {
				handleError(w, http.StatusInternalServerError, err)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	w.WriteHeader(http.StatusBadRequest)
}
