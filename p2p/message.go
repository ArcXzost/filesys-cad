package p2p

const (
	IncomingMessage = 1
	IncomingStream  = 2
)

type RPC struct {
	From    string
	Payload []byte
	Stream  bool
	Type    int // Add a type field to distinguish message types
	// Version int // Add a version number
}
