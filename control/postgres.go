package control

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const schema = `
CREATE TABLE __ (
	id text NOT NULL,
	name text NOT NULL,
	alias text NOT NULL,
	os text NOT NULL,
	dirs_conf json NOT NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	verified_at timestamp,
	pinged_at timestamp,
	loaded_at timestamp,
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
	ID         string      `db:"id"`
	Name       string      `db:"name"`
	Alias      string      `db:"alias"`
	OS         string      `db:"os"`
	DirsConf   *DirsConf   `db:"dirs_conf"`
	CreatedAt  time.Time   `db:"created_at"`
	UpdatedAt  time.Time   `db:"updated_at"`
	VerifiedAt pq.NullTime `db:"verified_at"`
	PingedAt   pq.NullTime `db:"pinged_at"`
	LoadedAt   pq.NullTime `db:"loaded_at"`
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
		return fmt.Errorf("Expected []byte but got %T", src)
	}
	return json.Unmarshal(source, c)
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
		return fmt.Errorf("Expected []byte but got %T", src)
	}
	return json.Unmarshal(source, c)
}

type clientCache struct {
	clients map[string][]string
	lock    sync.RWMutex
}

func (c *clientCache) getApprovedDatasetNames(clientID string) []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if names, ok := c.clients[clientID]; ok {
		return names
	}
	return nil
}

func (c *clientCache) build(datasets []*Dataset) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clients = make(map[string][]string)
	for _, d := range datasets {
		if !d.ClientID.Valid {
			continue
		}
		c.clients[d.ClientID.String] = append(
			c.clients[d.ClientID.String], d.Name,
		)
	}
}

func (c *clientCache) rebuild(clientID string, datasets []*Dataset) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clients[clientID] = make([]string, len(datasets))
	for i, d := range datasets {
		c.clients[clientID][i] = d.Name
	}
	return
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

	db    *sqlx.DB
	cache *clientCache
}

// NewPostgres creates a new Postgres instance
func NewPostgres(
	port uint, host, name, user, pass, clientsTable, datasetsTable string,
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
	}
	return
}

func (p *Postgres) create() {
	if err := p.connect(); err != nil {
		panic(err)
	}
	defer p.disconnect()
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
	defer p.disconnect()
	p.db.MustExec(fmt.Sprintf(`
		DROP TABLE %s;
		DROP TABLE %s;
	`, p.datasetsTable, p.clientsTable))
}

func (p *Postgres) connect() (err error) {
	if p.db != nil {
		return
	}
	p.db, err = sqlx.Connect(
		"postgres",
		fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			p.host, p.port, p.user, p.pass, p.name,
		),
	)
	if p.cache == nil {
		p.cache = &clientCache{
			clients: make(map[string][]string),
		}
		if datasets, err := p.getDatasets(); err == nil {
			p.cache.rebuild("", datasets)
		}
	}
	return
}

func (p *Postgres) disconnect() error {
	if p.db == nil {
		return nil
	}
	err := p.db.Close()
	p.db = nil
	return err
}

func (p *Postgres) initClient(id, name, os string) (err error) {
	_, err = p.db.NamedExec(
		fmt.Sprintf(`
			INSERT INTO %s
			(id, name, alias, os, dirs_conf, created_at, updated_at, pinged_at)
			VALUES (:id, :name, '', :os, '{}', :now, :now, :now)
		`, p.clientsTable),
		map[string]interface{}{
			"id":   id,
			"name": name,
			"os":   os,
			"now":  time.Now(),
		},
	)
	return
}

func (p *Postgres) getClientByID(id string) (client *Client, err error) {
	client = &Client{}
	err = p.db.Get(client, fmt.Sprintf(`SELECT * FROM %s WHERE id=$1`, p.clientsTable), id)
	return
}

func (p *Postgres) getDatasetsByClient(clientID string) (datasets []*Dataset, err error) {
	p.db.Select(
		&datasets,
		fmt.Sprintf(`SELECT * FROM %s WHERE client_id=$1`, p.datasetsTable),
		clientID,
	)
	return
}

func (p *Postgres) getDatasetByName(name string) (dataset *Dataset, err error) {
	dataset = &Dataset{}
	err = p.db.Get(dataset, fmt.Sprintf(`SELECT * FROM %s WHERE name=$1`, p.datasetsTable), name)
	return
}

func (p *Postgres) getDatasets() (datasets []*Dataset, err error) {
	p.db.Select(&datasets, fmt.Sprintf(`SELECT * FROM %s`, p.datasetsTable))
	return
}

func (p *Postgres) setPingedTime(clientID string, when time.Time) (err error) {
	_, err = p.db.NamedExec(fmt.Sprintf(`
		UPDATE %s SET pinged_at=:t WHERE id=:id
	`, p.clientsTable), map[string]interface{}{
		"id": clientID,
		"t":  when,
	})
	return
}

func (p *Postgres) setLoadedTime(clientID string, when time.Time) (err error) {
	_, err = p.db.NamedExec(fmt.Sprintf(`
		UPDATE %s SET loaded_at=:t WHERE id=:id
	`, p.clientsTable), map[string]interface{}{
		"id": clientID,
		"t":  when,
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
	defer p.disconnect()
	status |= sts.ClientIsDisabled
	var client *Client
	client, err = p.getClientByID(clientID)
	if err == sql.ErrNoRows {
		if err = p.initClient(clientID, clientName, clientOS); err != nil {
			return
		}
		if client, err = p.getClientByID(clientID); err != nil {
			return
		}
	}
	if err = p.setPingedTime(clientID, time.Now()); err != nil {
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
	defer p.disconnect()
	client, err := p.getClientByID(clientID)
	if client == nil || err != nil {
		return
	}
	datasets, err := p.getDatasetsByClient(clientID)
	if err != nil || len(datasets) == 0 {
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
	p.cache.rebuild(clientID, datasets)
	return
}

// SetClientConfReceived helps fulfill the sts.ClientManager interface
func (p *Postgres) SetClientConfReceived(clientID string, when time.Time) error {
	if err := p.connect(); err != nil {
		return err
	}
	defer p.disconnect()
	return p.setLoadedTime(clientID, when)
}

// IsValid fulfills sts.IsKeyValid and determines whether or not the input key
// (client ID) is allowed to send a file that looks like the input file struct
func (p *Postgres) IsValid(dataset, clientID string) bool {
	if p.cache == nil {
		// A connection will build the cache
		p.connect()
		p.disconnect()
	}
	for _, name := range p.cache.getApprovedDatasetNames(clientID) {
		if name == dataset {
			return true
		}
	}
	return false
}
