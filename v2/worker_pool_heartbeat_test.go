package work

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Client_WorkerPoolHeartbeats(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	heartbeat := client.mockWorkerPoolHeartbeat()

	beats, err := client.WorkerPoolHeartbeats()
	if err != nil {
		t.Fatalf("should not received err, but got %+v", err)
	}
	if len(beats) != 1 {
		t.Fatalf("should received only 1 heartbeat, but got too many %v", beats)
	}
	assert.Equal(t, heartbeat, beats[0])
}
