package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
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
	statusBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}
	status64, err := strconv.ParseUint(string(statusBytes), 10, 64)
	status = sts.ClientStatus(status64)
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
	confBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}
	conf = &sts.ClientConf{}
	err = json.Unmarshal(confBytes, conf)
	return
}

// SetClientConfReceived to fulfil sts.ClientManager interface
func (c *Client) SetClientConfReceived(clientID string, when time.Time) error {
	// N/A currently
	return nil
}

// routeClientManagement handles the HTTP routes for getting a client's status
// and configuration
func (s *Server) routeClientManagement(w http.ResponseWriter, r *http.Request) {
	log.Debug("Handling:", r.URL.RequestURI)
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
				log.Error(err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
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
				log.Error(err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			var respJSON []byte
			respJSON, err = json.Marshal(config)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			log.Debug("Client conf:", string(respJSON))
			w.Header().Set(HeaderContentType, HeaderJSON)
			if err = s.respond(w, http.StatusOK, respJSON); err != nil {
				log.Error(err.Error())
			}
			s.ClientManager.SetClientConfReceived(clientID, time.Now())
			return
		}
	}
	w.WriteHeader(http.StatusBadRequest)
}
