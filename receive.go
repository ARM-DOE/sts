package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/httputils"
	"github.com/ARM-DOE/sts/logging"
	"github.com/ARM-DOE/sts/pathutils"
)

// PartExt is the file extension added to files in the stage area as they are
// received.
const PartExt = ".part"

// ReceiverConf struct contains configuration parameters needed to run.
type ReceiverConf struct {
	StageDir string
	FinalDir string
	Port     int
	TLSCert  string
	TLSKey   string
}

// Receiver struct contains the configuration and finalizer references.
type Receiver struct {
	Conf      *ReceiverConf
	lock      sync.Mutex
	fileLocks map[string]*sync.Mutex
	finalizer *Finalizer
}

// NewReceiver creates new receiver instance according to the input configuration.
// A reference to a finalizer instance is necessary in order for the validation route
// to be able to confirm files received.
func NewReceiver(conf *ReceiverConf, f *Finalizer) *Receiver {
	rcv := &Receiver{}
	rcv.Conf = conf
	rcv.fileLocks = make(map[string]*sync.Mutex)
	rcv.finalizer = f
	return rcv
}

// Serve starts HTTP server.
func (rcv *Receiver) Serve() {
	http.HandleFunc("/data", rcv.routeData)
	http.HandleFunc("/validate", rcv.routeValidate)

	var err error
	addr := fmt.Sprintf(":%d", rcv.Conf.Port)
	if rcv.Conf.TLSCert != "" && rcv.Conf.TLSKey != "" {
		_, err = httputils.ServeTLS(addr, rcv.Conf.TLSCert, rcv.Conf.TLSKey)
	} else {
		_, err = httputils.Serve(addr)
	}
	if err != nil {
		panic(err.Error())
	}
}

func (rcv *Receiver) getLock(path string) *sync.Mutex {
	rcv.lock.Lock()
	defer rcv.lock.Unlock()
	_, exists := rcv.fileLocks[path]
	if !exists {
		rcv.fileLocks[path] = &sync.Mutex{}
	}
	return rcv.fileLocks[path]
}

func (rcv *Receiver) routeValidate(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(HeaderSourceName)
	files := []*ConfirmFile{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&files)
	if err != nil {
		fmt.Fprint(w, http.StatusBadRequest) // Respond with a 400
		return
	}
	respMap := make(map[string]int, len(files))
	for _, f := range files {
		success, found := rcv.finalizer.IsFinal(source, f.RelPath, f.Started)
		code := ConfirmNone
		if found && success {
			code = ConfirmPassed
		} else if found && !success {
			code = ConfirmFailed
		}
		respMap[f.RelPath] = code
	}
	respJSON, _ := json.Marshal(respMap)
	w.WriteHeader(http.StatusOK)
	w.Write(respJSON)
}

func (rcv *Receiver) routeData(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	compressed := r.Header.Get("Content-Encoding") == "gzip"
	source := r.Header.Get(HeaderSourceName)
	meta := r.Header.Get(HeaderBinData)
	reader, err := NewBinReader(meta, r.Body, compressed)
	if err != nil {
		logging.Error(err.Error())
		fmt.Fprint(w, http.StatusPartialContent) // respond with a 206
	}
	for {
		next, eof := reader.Next()
		if eof { // Reached end of multipart file
			break
		}
		// TODO: Keep track of any succeessfully handled parts and return that count
		// to the sender so those aren't resent.
		err := rcv.parsePart(next, source)
		if err != nil {
			logging.Error(err.Error())
			fmt.Fprint(w, http.StatusPartialContent) // respond with a 206
			return
		}
	}
	fmt.Fprint(w, http.StatusOK)
}

func (rcv *Receiver) initStageFile(filePath string, size int64) {
	os.MkdirAll(path.Dir(filePath), os.ModePerm)
	fh, err := os.Create(filePath + PartExt)
	logging.Debug(fmt.Sprintf("RECEIVE Creating Empty File: %s (%d B)", filePath, size))
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to create empty file at %s.%s with size %d: %s", filePath, PartExt, size, err.Error()))
	}
	fh.Truncate(size)
	fh.Close()
}

func (rcv *Receiver) parsePart(pr *PartReader, source string) (err error) {

	logging.Debug("RECEIVE Process Part:", pr.Meta.Path, pr.Meta.FileSize)

	path := pathutils.Join(rcv.Conf.StageDir, source, pr.Meta.Path)
	hash := fileutils.NewMD5()
	lock := rcv.getLock(path)

	lock.Lock()
	_, err = os.Stat(path + PartExt)
	if os.IsNotExist(err) {
		rcv.initStageFile(path, pr.Meta.FileSize)
	}
	lock.Unlock()

	// Read the part and write it to the right place in the staged "partial"
	fh, err := os.OpenFile(path+PartExt, os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("Failed to open file while trying to write part: %s", err.Error())
	}
	fh.Seek(pr.Meta.Start, 0)
	logging.Debug("RECEIVE Seek:", pr.Meta.Path, pr.Meta.Start)
	bytes := make([]byte, hash.BlockSize)
	var nb int
	wrote := 0
	for {
		// The number of bytes read can often be less than the size of the passed buffer.
		nb, err = pr.Read(bytes)
		wrote += nb
		hash.Update(bytes[0:nb])
		fh.Write(bytes[0:nb])
		if err != nil {
			logging.Debug("RECEIVE:", err.Error())
			break
		}
	}
	logging.Debug("RECEIVE Wrote:", pr.Meta.Path, wrote)
	fh.Close()
	if pr.Meta.Hash != hash.Sum() {
		return fmt.Errorf("Bad part of %s from bytes %d:%d (%s:%s)", pr.Meta.Path, pr.Meta.Start, pr.Meta.End, pr.Meta.Hash, hash.Sum())
	}

	// Make sure we're the only one updating the companion
	lock.Lock()
	defer lock.Unlock()

	// Update the companion file
	cmp, err := NewCompanion(source, path, pr.Meta.FileSize)
	if err != nil {
		return err
	}
	err = cmp.AddPart(pr.Meta.FileHash, pr.Meta.Hash, pr.Meta.PrevPath, pr.Meta.Start, pr.Meta.End)
	if err != nil {
		return fmt.Errorf("Failed to add part to companion: %s", err.Error())
	}
	if cmp.IsComplete() {

		// Finish file by updating the modtime and removing the "partial" extension.
		err := os.Rename(path+PartExt, path)
		if err != nil {
			return err
		}
		os.Chtimes(path, time.Now(), time.Now()) // Update mtime so that scanner will pick up the file
		logging.Debug("RECEIVE File Done:", path)
	}
	logging.Debug("RECEIVE Park OK:", path)
	return nil
}
