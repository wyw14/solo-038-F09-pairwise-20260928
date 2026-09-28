package journal

import (
	"errors"
	"sync"
)

type State struct {
	mu          sync.RWMutex
	sequence    uint64
	durable     uint64
	lastFailure string
}

// Seed raises the issued/durable counters to a recovered high-water mark so
// sequences issued after a restart never collide with pre-crash ones.
func (state *State) Seed(sequence uint64) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if sequence > state.sequence {
		state.sequence = sequence
	}
	if sequence > state.durable {
		state.durable = sequence
	}
}

func (state *State) Issued() uint64 {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.sequence++
	return state.sequence
}

func (state *State) MarkDurable(sequence uint64) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if sequence == 0 || sequence > state.sequence {
		return errors.New("journal sequence is outside issued range")
	}
	if sequence > state.durable {
		state.durable = sequence
	}
	state.lastFailure = ""
	return nil
}

func (state *State) MarkFailure(message string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.lastFailure = message
}

func (state *State) Snapshot() (uint64, uint64, string) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.sequence, state.durable, state.lastFailure
}

func (state *State) IsDurable(sequence uint64) bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return sequence > 0 && sequence <= state.durable
}
