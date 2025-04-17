package control

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/log"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

const schema = `
CREATE TABLE __ (
	id text NOT NULL,
	upload_key text NOT NULL,
	name text NOT NULL,
	alias text NOT NULL,
	os text NOT NULL,
	dirs_conf json NOT NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	verified_at timestamp,
	pinged_at timestamp,
	loaded_at timestamp,
    state json,
	PRIMARY KEY (id)
);
CREATE TABLE __ (
	name text NOT NULL,
	source_conf json NOT NULL,
	client_id text,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	PRIMARY KEY (name),
	FOREIGN KEY (client_id) REFERENCES clients(id)
);
`

// Client represents a client record
type Client struct {
	ID         string           `db:"id"`
	Key        string           `db:"upload_key"`
	Name       string           `db:"name"`
	Alias      string           `db:"alias"`
	OS         string           `db:"os"`
	DirsConf   *DirsConf        `db:"dirs_conf"`
	State      *sts.ClientState `db:"state"`
	CreatedAt  time.Time        `db:"created_at"`
	UpdatedAt  time.Time        `db:"updated_at"`
	VerifiedAt sql.NullTime     `db:"verified_at"`
	PingedAt   sql.NullTime     `db:"pinged_at"`
	LoadedAt   sql.NullTime     `db:"loaded_at"`
}

// Dataset represents a dataset record
type Dataset struct {
	Name       string         `db:"name"`
	SourceConf *SourceConf    `db:"source_conf"`
	ClientID   sql.NullString `db:"client_id"`
	CreatedAt  time.Time      `db:"created_at"`
	UpdatedAt  time.Time      `db:"updated_at"`
}

// DirsConf wraps the sts.ClientDirs type
type DirsConf struct {
	sts.ClientDirs
}

// Value transforms this type to database driver-compatible type
func (c *DirsConf) Value() (driver.Value, error) {
	return json.Marshal(&c.ClientDirs)
}

// Scan transforms the database type to our internal type
func (c *DirsConf) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte but got %T", src)
	}
	err := json.Unmarshal(source, c)
	if err != nil {
		// Because sqlx does not report this error at all AFAICT
		log.Error("Directory JSON Error! %s", err.Error())
	}
	return err
}

// SourceConf wraps the sts.SourceConf type
type SourceConf struct {
	sts.SourceConf
}

// Value transforms this type to database driver-compatible type
func (c *SourceConf) Value() (driver.Value, error) {
	return json.Marshal(c)
}

// Scan transforms the database type to our internal type
func (c *SourceConf) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte but got %T", src)
	}
	err := json.Unmarshal(source, c)
	if err != nil {
		// Because sqlx does not report this error at all AFAICT
		log.Error("Source JSON Error! %s", err.Error())
	}
	return err
}

type clientCache struct {
	datasets    map[string]string
	clientKeys  map[string]string
	clientNames map[string]string
	lock        sync.RWMutex
}

func (c *clientCache) getClientKey(id string) (key string, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	key, ok = c.clientKeys[id]
	return
}

func (c *clientCache) getClientName(id string) (name string, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	name, ok = c.clientNames[id]
	return
}

func (c *clientCache) getDatasetClientID(name string) (id string, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	id, ok = c.datasets[name]
	return
}

func (c *clientCache) build(clients []*Client, datasets []*Dataset) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clientKeys = make(map[string]string)
	c.clientNames = make(map[string]string)
	c.datasets = make(map[string]string)
	log.Info("Building Client Cache ...")
	for _, client := range clients {
		log.Info("Adding Client to Cache:", client.Name, "ID:", client.ID, "Key:", client.Key)
		c.clientKeys[client.ID] = client.Key
		c.clientNames[client.ID] = client.Name
	}
	for _, dataset := range datasets {
		if !dataset.ClientID.Valid {
			continue
		}
		log.Info(
			"Adding Dataset (Source) to Cache:", dataset.Name,
			"Client ID:", dataset.ClientID.String,
		)
		c.datasets[dataset.Name] = dataset.ClientID.String
	}
}

func (c *clientCache) rebuild(client *Client, datasets []*Dataset) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clientKeys[client.ID] = client.Key
	c.clientNames[client.ID] = client.Name
	for _, d := range datasets {
		c.datasets[d.Name] = client.ID
	}
}

// Postgres fulfills the sts.ClientManager interface
type Postgres struct {
	host string
	port uint
	name string
	user string
	pass string

	clientsTable  string
	datasetsTable string

	idDecoder sts.DecodeClientID

	db    *sqlx.DB
	cache *clientCache
}

// NewPostgres creates a new Postgres instance
func NewPostgres(
	port uint, host, name, user, pass,
	clientsTable, datasetsTable string,
	idDecoder sts.DecodeClientID,
) (p *Postgres) {
	if port == 0 {
		port = 5432
	}
	if clientsTable == "" {
		clientsTable = "clients"
	}
	if datasetsTable == "" {
		datasetsTable = "datasets"
	}
	p = &Postgres{
		host:          host,
		port:          port,
		name:          name,
		user:          user,
		pass:          pass,
		clientsTable:  clientsTable,
		datasetsTable: datasetsTable,
		idDecoder:     idDecoder,
	}
	return
}

func (p *Postgres) create() {
	if err := p.connect(); err != nil {
		panic(err)
	}
	p.db.MustExec(
		strings.Replace(
			strings.Replace(
				schema, "__", p.clientsTable, 1,
			), "__", p.datasetsTable, 1,
		),
	)
}

func (p *Postgres) destroy() {
	if err := p.connect(); err != nil {
		panic(err)
	}
	p.db.MustExec(fmt.Sprintf(`
		DROP TABLE %s;
		DROP TABLE %s;
	`, p.datasetsTable, p.clientsTable))
}

func (p *Postgres) connect() (err error) {
	if p.db != nil {
		return
	}
    sslmode := "require"
    if p.host == "localhost" {
        sslmode = "disable"
    }
	p.db, err = sqlx.Connect(
		"pgx",
		fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			p.host, p.port, p.user, p.pass, p.name, sslmode,
		),
	)
	if p.cache == nil {
		p.cache = &clientCache{}
		if datasets, err := p.getDatasets(); err == nil {
			if clients, err := p.getClients(); err == nil {
				p.cache.build(clients, datasets)
			}
		}
	}
	return
}

func (p *Postgres) initClient(id, key, name, os string) (err error) {
	log.Debug("Initializing Client:", id, key, name, os)
	_, err = p.db.NamedExec(
		fmt.Sprintf(`
			INSERT INTO %s
			(id, upload_key, name, alias, os, dirs_conf, created_at, updated_at, pinged_at)
			VALUES (:id, :key, :name, '', :os, '{}', :now, :now, :now)
		`, p.clientsTable),
		map[string]interface{}{
			"id":   id,
			"key":  key,
			"name": name,
			"os":   os,
			"now":  time.Now(),
		},
	)
	return
}

func (p *Postgres) getClients() (clients []*Client, err error) {
	err = p.db.Select(
		&clients,
		fmt.Sprintf(`
            SELECT id, upload_key, name, alias, os, dirs_conf,
            created_at, updated_at, pinged_at, loaded_at, verified_at
			FROM %s
        `, p.clientsTable),
	)
	return
}

func (p *Postgres) getClientByID(id string) (client *Client, err error) {
	client = &Client{}
	err = p.db.Get(client, fmt.Sprintf(`
		SELECT id, upload_key, name, alias, os, dirs_conf,
		created_at, updated_at, pinged_at, loaded_at, verified_at
		FROM %s
		WHERE id=$1
	`, p.clientsTable), id)
	return
}

func (p *Postgres) getDatasetsByClient(uid string) (datasets []*Dataset, err error) {
	err = p.db.Select(
		&datasets,
		fmt.Sprintf(`
			SELECT name, source_conf, client_id, created_at, updated_at
			FROM %s
			WHERE client_id=$1
		`, p.datasetsTable),
		uid,
	)
	return
}

// Not used...
// func (p *Postgres) getDatasetByName(name string) (dataset *Dataset, err error) {
// 	dataset = &Dataset{}
// 	err = p.db.Get(dataset, fmt.Sprintf(`
// 		SELECT name, source_conf, client_id, created_at, updated_at
// 		FROM %s
// 		WHERE name=$1
// 	`, p.datasetsTable), name)
// 	return
// }

func (p *Postgres) getDatasets() (datasets []*Dataset, err error) {
	err = p.db.Select(&datasets, fmt.Sprintf(`
		SELECT name, source_conf, client_id, created_at, updated_at
		FROM %s
	`, p.datasetsTable))
	return
}

func (p *Postgres) setPingedTime(uid string, when time.Time) (err error) {
	_, err = p.db.NamedExec(fmt.Sprintf(`
		UPDATE %s SET pinged_at=:t WHERE id=:id
	`, p.clientsTable), map[string]interface{}{
		"id": uid,
		"t":  when,
	})
	return
}

func (p *Postgres) setLoadedTime(uid string, when time.Time) (err error) {
	_, err = p.db.NamedExec(fmt.Sprintf(`
		UPDATE %s SET loaded_at=:t WHERE id=:id
	`, p.clientsTable), map[string]interface{}{
		"id": uid,
		"t":  when,
	})
	return
}

func (p *Postgres) setState(uid string, state sts.ClientState) (err error) {
	var encoded []byte
	if encoded, err = json.Marshal(state); err != nil {
		return
	}
	_, err = p.db.NamedExec(fmt.Sprintf(`
		UPDATE %s SET state=:state WHERE id=:id
	`, p.clientsTable), map[string]interface{}{
		"id":    uid,
		"state": string(encoded),
	})
	return
}

// GetClientStatus helps fulfill the sts.ClientManager interface
func (p *Postgres) GetClientStatus(
	clientID,
	clientName,
	clientOS string,
) (status sts.ClientStatus, err error) {
	if err = p.connect(); err != nil {
		return
	}
	key, uid := p.idDecoder(clientID)
	status |= sts.ClientIsDisabled
	var client *Client
	client, err = p.getClientByID(uid)
	if err == sql.ErrNoRows {
		if err = p.initClient(uid, key, clientName, clientOS); err != nil {
			return
		}
		if client, err = p.getClientByID(uid); err != nil {
			return
		}
	} else if err != nil {
		return
	} else if client.Key != key {
		err = fmt.Errorf(
			"mismatched keys (expected '%s' but got '%s')", client.Key, key,
		)
		return
	}
	if err = p.setPingedTime(uid, time.Now()); err != nil {
		return
	}
	if !client.VerifiedAt.Valid {
		return
	}
	status = sts.ClientIsApproved
	if client.LoadedAt.Valid && client.LoadedAt.Time.After(client.UpdatedAt) {
		return
	}
	status |= sts.ClientHasUpdatedConfiguration
	return
}

// GetClientConf helps fulfill the sts.ClientManager interface
func (p *Postgres) GetClientConf(clientID string) (conf *sts.ClientConf, err error) {
	if err = p.connect(); err != nil {
		return
	}
	_, uid := p.idDecoder(clientID)
	client, err := p.getClientByID(uid)
	if client == nil || err != nil {
		return
	}
	datasets, err := p.getDatasetsByClient(uid)
	if err != nil {
		return
	}
	conf = &sts.ClientConf{}
	if client.DirsConf != nil {
		conf.Dirs = &client.DirsConf.ClientDirs
	}
	if conf.Dirs == nil {
		conf.Dirs = &sts.ClientDirs{}
	}
	for _, dataset := range datasets {
		conf.Sources = append(conf.Sources, &dataset.SourceConf.SourceConf)
	}
	// Whenever a client gets an updated conf, let's rebuild the cache to make
	// sure we're current
	p.cache.rebuild(client, datasets)
	return
}

// SetClientConfReceived helps fulfill the sts.ClientManager interface
func (p *Postgres) SetClientConfReceived(clientID string, when time.Time) error {
	if err := p.connect(); err != nil {
		return err
	}
	_, uid := p.idDecoder(clientID)
	return p.setLoadedTime(uid, when)
}

// SetClientState helps fulfill the sts.ClientManager interface
func (p *Postgres) SetClientState(clientID string, state sts.ClientState) error {
	if err := p.connect(); err != nil {
		return err
	}
	_, uid := p.idDecoder(clientID)
	return p.setState(uid, state)
}

// IsValid fulfills sts.IsKeyValid and determines whether or not the input key
// (client ID) is allowed to send a file that looks like the input file struct
func (p *Postgres) IsValid(source, clientID string) bool {
	if p.cache == nil {
		// A connection will build the cache
		_ = p.connect()
	}
	key, cid := p.idDecoder(clientID)
	if k, ok := p.cache.getClientKey(cid); !ok || k != key {
		return false
	}
	if cname, ok := p.cache.getClientName(cid); ok && cname == source {
		// This only happens in a hybrid scenario where the "source" is the
		// name of the client and not the dataset.  In this case, the client
		// is using manual configuration provided by the DAP and NOT automated
		// config provided by the server.
		return true
	}
	if id, ok := p.cache.getDatasetClientID(source); !ok || id != cid {
		return false
	}
	return true
}
