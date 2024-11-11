package control

// import (
// 	"database/sql"
// 	"os"
// 	"path/filepath"
// 	"time"

// 	"github.com/arm-doe/sts"
// 	"github.com/arm-doe/sts/fileutil"
// )

// const (
// 	clientJSON = "client.json"
// 	confYAML   = "conf.yaml"
// )

// type client struct {
// 	ID         string          `json:"id"`
// 	Name       string          `json:"name"`
// 	OS         string          `json:"os"`
// 	Conf       *sts.ClientConf `json:"conf"`
// 	CreatedAt  time.Time       `json:"created_at"`
// 	UpdatedAt  time.Time       `json:"updated_at"`
// 	ApprovedAt time.Time       `json:"approved_at"`
// 	PingedAt   time.Time       `json:"pinged_at"`
// 	LoadedAt   time.Time       `json:"loaded_at"`
// }

// // Filed fulfills the sts.ClientManager interface
// type Filed struct {
// 	RootPath string
// }

// func (f *Filed) create() {
// 	var err error
// 	if f.RootPath, err = fileutil.InitPath("", f.RootPath, true); err != nil {
// 		panic(err)
// 	}
// }

// func (f *Filed) destroy() {
// 	if err := os.RemoveAll(f.RootPath); err != nil {
// 		panic(err)
// 	}
// }

// func (f *Filed) initClient(id, name, os string) (err error) {
// 	path := filepath.Join(f.RootPath, id, clientJSON)
// 	err = fileutil.WriteJSON(path, map[string]interface{}{
// 		"id":         id,
// 		"name":       name,
// 		"os":         os,
// 		"created_at": time.Now(),
// 		"updated_at": time.Now(),
// 		"pinged_at":  time.Now(),
// 	})
// 	return
// }

// func (f *Filed) getClientByID(id string) (c *client, err error) {
// 	c = &client{}
// 	path := filepath.Join(f.RootPath, id, clientJSON)
// 	err = fileutil.LoadJSON(path, c)
// 	return
// }

// func (f *Filed) setLoadedTime(clientID string, when time.Time) (err error) {
// 	path := filepath.Join(f.RootPath, clientID, clientJSON)
// 	var c *client
// 	if c, err = f.getClientByID(clientID); err != nil {
// 		return
// 	}
// 	c.LoadedAt = when
// 	err = fileutil.WriteJSON(path, c)
// 	return
// }

// // GetClientStatus helps fulfill the sts.ClientManager interface
// func (f *Filed) GetClientStatus(
// 	clientID,
// 	clientName,
// 	clientOS string,
// ) (status sts.ClientStatus, err error) {
// 	status = sts.ClientIsDisabled
// 	var client *client
// 	client, err = f.getClientByID(clientID)
// 	if err == sql.ErrNoRows {
// 		if err = f.initClient(clientID, clientName, clientOS); err != nil {
// 			return
// 		}
// 		if client, err = f.getClientByID(clientID); err != nil {
// 			return
// 		}
// 	}
// 	if !client.ApprovedAt.Valid {
// 		return
// 	}
// 	status = sts.ClientIsApproved
// 	if client.LoadedAt.Valid && client.LoadedAt.Time.Before(client.UpdatedAt) {
// 		status |= sts.ClientHasUpdatedConfiguration
// 	}
// 	return
// }

// // GetClientConf helps fulfill the sts.ClientManager interface
// func (f *Filed) GetClientConf(clientID string) (conf *sts.ClientConf, err error) {
// 	client, err := f.getClientByID(clientID)
// 	if err != nil {
// 		return
// 	}
// 	conf = &sts.ClientConf{
// 		Dirs: &client.DirsConf.ClientDirs,
// 	}
// 	datasets, err := f.getDatasetsByClient(clientID)
// 	if err != nil {
// 		return
// 	}
// 	for _, dataset := range datasets {
// 		conf.Sources = append(conf.Sources, &dataset.SourceConf.SourceConf)
// 	}
// 	return
// }

// // SetClientConfReceived helps fulfill the sts.ClientManager interface
// func (f *Filed) SetClientConfReceived(clientID string, when time.Time) error {
// 	return f.setLoadedTime(clientID, when)
// }
