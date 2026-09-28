package dryer

import "fmt"

// Recovery is the durable dryer request recovered after restart. It carries
// the target/profile the operator last asked for, not a claim that the dryer
// is hot.
type Recovery struct {
	JobID      string
	Generation uint64
	Target     float64
	Profile    string
}

// Restore records the recovered dryer request but always leaves the heater
// inactive (active=false, current=0). After a power loss the thermal state is
// unknown, so heat must be re-armed manually rather than resumed.
func Restore(state *State, recovery Recovery) error {
	if recovery.JobID == "" || recovery.Generation == 0 || recovery.Profile == "" {
		return fmt.Errorf("dryer recovery fence is incomplete")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.jobID = recovery.JobID
	state.jobGen = recovery.Generation
	state.target = recovery.Target
	state.profile = recovery.Profile
	state.current = 0
	state.active = false
	return nil
}
