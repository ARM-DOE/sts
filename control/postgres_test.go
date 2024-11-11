package control

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/mock"
)

var hasDatabase = flag.Bool("db", false, strings.TrimSpace(`
Current system has a local Postgres server with database named "sts"
`))

func decodeClientID(clientID string) (string, string) {
	return clientID, ""
}

func TestSQL(t *testing.T) {
	if !*hasDatabase {
		return
	}
	log.InitExternal(&mock.Logger{DebugMode: true})
	var err error
	p := NewPostgres(uint(0), "localhost", "sts", "sts_admin", "sts", "", "", decodeClientID)
	p.destroy()
	p.create()
	p.connect()
	if err = p.initClient("somerandomstring", "anotherrandomstring", "somerandomname", "darwin"); err != nil {
		t.Fatal(err)
	}
	_, err = p.db.Exec(`
		INSERT INTO datasets
		(name, source_conf, client_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
	`, "name", `{}`, "somerandomstring", time.Now(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	datasets, err := p.getDatasets()
	if err != nil || len(datasets) == 0 {
		t.Fatal(err)
	}
}
