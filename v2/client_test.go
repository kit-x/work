package v2

import (
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

func Test_Client_WorkerPoolHeartbeats(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	poolID := "a"
	_ = client.setWorkerPoolIDs(poolID)

	heartbeat := &WorkerPoolHeartbeat{
		WorkerPoolID: poolID,
		StartedAt:    time.Now().Unix(),
		HeartbeatAt:  time.Now().Unix(),
		JobNames:     []string{"a", "b", "c"},
		Concurrency:  1,
		Host:         "host",
		Pid:          1,
		WorkerIDs:    []string{"w-a", "w-b", "w-c"},
	}
	_ = client.setWorkerPoolHeartbeat(heartbeat)

	beats, err := client.WorkerPoolHeartbeats()
	if err != nil {
		t.Fatalf("should not received err, but got %+v", err)
	}
	if len(beats) != 1 {
		t.Fatalf("should received only 1 heartbeat, but got too many %v", beats)
	}
	assert.Equal(t, heartbeat, beats[0])
}
