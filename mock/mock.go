package mock

import "time"

// File is meant to implement as many o the file-typed interfaces as possible
type File struct {
	Name     string
	Path     string
	Size     int64
	Time     time.Time
	Hash     string
	SendTime int64
}

// GetPath gets the path
func (f *File) GetPath() string {
	return f.Path
}

// GetRelPath gets the name part of the path
func (f *File) GetRelPath() string {
	return f.Name
}

// GetName is the same as GetRelPath
func (f *File) GetName() string {
	return f.Name
}

// GetSize gets the file size
func (f *File) GetSize() int64 {
	return f.Size
}

// GetTime gets the file mod time
func (f *File) GetTime() int64 {
	return f.Time.Unix()
}

// GetHash gets the file signature
func (f *File) GetHash() string {
	return f.Hash
}

// TimeMs returns the time (in milliseconds) a file took to be sent
func (f *File) TimeMs() int64 {
	return f.SendTime
}
