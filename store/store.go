package store

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"filesys-cad/crypto"
)

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blockSize := 5
	sliceLen := len(hashStr) / blockSize

	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blockSize, (i*blockSize)+blockSize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	PathName string
	Filename string
}

func (p *PathKey) fullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Filename)
}

type StoreOpts struct {
	// Root is where all the files of a fileserver will be stored
	Root              string
	PathTransformFunc PathTransformFunc
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		Filename: key,
	}
}

// Add to store/store.go

// Add a map to track open file handles
type Store struct {
	StoreOpts
}

// Update NewStore to initialize the map
func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	return &Store{
		StoreOpts: opts,
	}
}

// Update readStream to track file handles
func (s *Store) readStream(encKey []byte, id string, key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.fullPath())

	file, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return 0, nil, err
	}

	// Create a pipe for streaming
	pr, pw := io.Pipe()

	// Start a goroutine to handle the decryption
	go func() {
		defer file.Close()
		defer pw.Close()

		if _, err := crypto.CopyDecrypt(encKey, file, pw); err != nil {
			pw.CloseWithError(err)
		}
	}()

	return fi.Size(), pr, nil
}

func (s *Store) Has(id string, key string) bool {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.fullPath())
	_, err := os.Stat(fullPathWithRoot)
	// fmt.Printf("path: %s, error: %+v\n", fullPathWithRoot, err)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Delete(id string, key string) error {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.fullPath())

	// Remove the file first
	if err := os.Remove(fullPathWithRoot); err != nil {
		return err
	}

	// Remove all empty parent directories up to the root
	dir := path.Dir(fullPathWithRoot)
	for dir != fmt.Sprintf("%s/%s", s.Root, id) {
		if err := os.Remove(dir); err != nil {
			// Stop if the directory is not empty
			break
		}
		dir = path.Dir(dir)
	}

	log.Printf("[%s] Deleted file (%s)\n", s.Root, key)
	return nil
}

func (s *Store) Write(id string, key string, r io.Reader) (int64, error) {
	return s.writeStream(id, key, r)
}

func (s *Store) WriteDecrypt(encKey []byte, id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := crypto.CopyDecrypt(encKey, r, f)
	return int64(n), err
}

func (s *Store) WriteEncrypt(encKey []byte, id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := crypto.CopyEncrypt(encKey, r, f)
	return int64(n), err
}

func (s *Store) openFileForWriting(id string, key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)
	pathKeyWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.PathName)

	if err := os.MkdirAll(pathKeyWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.fullPath())

	file, err := os.Create(fullPathWithRoot)
	if err != nil {
		return nil, err
	}

	// Do NOT close the file here! Let the caller handle it.
	return file, nil
}

func (s *Store) writeStream(id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close() // Close the file after writing is done

	return io.Copy(f, r)
}

func (s *Store) Read(encKey []byte, id string, key string) (int64, io.Reader, error) {
	return s.readStream(encKey, id, key)
}
