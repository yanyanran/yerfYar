package client

import "bytes"

const defaultScratchSize = 64 * 1024

// Simple represents an instance of client connected to a set of yerkYar servers.
type Simple struct {
	addrs []string

	buf bytes.Buffer
}

// NewSimple create a new client for the yerkYar server.
func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
	}
}

// Send sends the messages to the yerkYar servers.
func (s *Simple) Send(msgs []byte) error {
	_, err := s.buf.Write(msgs)
	return err
}

// Receive will either wait for new messages or return an error in case something goes wrong.
// The scratch buffer can be used to read the data.
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	n, err := s.buf.Read(scratch)
	if err != nil {
		return nil, err
	}

	return scratch[0:n], nil
}