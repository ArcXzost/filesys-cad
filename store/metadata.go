package store

import "sync"

type MetadataStore struct {
	mu    sync.RWMutex
	files map[string]FileMetadata // Maps file keys to FileMetadata
}

type FileMetadata struct {
	Key             string
	ResponsiblePeer string   // Address of the responsible peer
	ReplicaPeers    []string // Addresses of the replica peers
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
