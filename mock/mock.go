package mock

import "time"

// Hashed implements sts.Hashed
type Hashed struct {
	Name string
	Path string
	Size int64
	Time time.Time
	Hash string
}

// GetPath implements sts.Hashed.GetPath
func (f *Hashed) GetPath() string {
	return f.Path
}

// GetRelPath implements sts.Hashed.GetRelPath
func (f *Hashed) GetRelPath() string {
	return f.Name
}

// GetSize implements sts.Hashed.GetSize
func (f *Hashed) GetSize() int64 {
	return f.Size
}

// GetTime implements sts.Hashed.GetTime
func (f *Hashed) GetTime() int64 {
	return f.Time.Unix()
}

// GetHash implements sts.Hashed.GetHash
func (f *Hashed) GetHash() string {
	return f.Hash
}
