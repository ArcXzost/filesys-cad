package store

import (
	"sync"
	"time"
)

type MetadataStore struct {
	mu    sync.RWMutex
	files map[string]FileMetadata // Maps file keys to FileMetadata
}

type FileMetadata struct {
	Key             string
	ResponsiblePeer string   // Address of the responsible peer
	ReplicaPeers    []string // Addresses of the replica peers
	Versions        []FileVersion
}

type FileVersion struct {
	VersionID string
	Timestamp time.Time
	Size      int64
	Hash      string // SHA-256 hash of the file content
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		files: make(map[string]FileMetadata),
	}
}

func (m *MetadataStore) AddFile(key, responsiblePeer string, replicaPeers []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[key] = FileMetadata{
		Key:             key,
		ResponsiblePeer: responsiblePeer,
		ReplicaPeers:    replicaPeers,
	}
}

func (m *MetadataStore) RemoveFile(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, key)
}

func (m *MetadataStore) GetFilesForPeer(peerAddr string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var files []string
	for key, metadata := range m.files {
		if metadata.ResponsiblePeer == peerAddr || contains(metadata.ReplicaPeers, peerAddr) {
			files = append(files, key)
		}
	}
	return files
}

func (m *MetadataStore) GetMetadataForFile(key string) (FileMetadata, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	metadata, exists := m.files[key]
	return metadata, exists
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (m *MetadataStore) ListFiles() []FileMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	files := make([]FileMetadata, 0, len(m.files))
	for _, metadata := range m.files {
		files = append(files, metadata)
	}
	return files
}

func (m *MetadataStore) UpdateVersions(key string, versions []FileVersion) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if metadata, exists := m.files[key]; exists {
		metadata.Versions = versions
		m.files[key] = metadata
	}
}

func (m *MetadataStore) GetVersions(key string) ([]FileVersion, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	metadata, exists := m.files[key]
	if !exists {
		return nil, false
	}
	return metadata.Versions, true
}
