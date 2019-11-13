package work

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Client_WorkerObservations(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	ob := client.mockWorkerObservation()

	got, err := client.WorkerObservations()
	if err != nil {
		t.Fatalf("should not received err, but got %+v", err)
	}
	if len(got) != len(ob.heartbeat.WorkerIDs) {
		t.Fatalf("should received only 1 worker ob, but got too many %v", got)
	}
	ob.heartbeat = nil
	assert.Equal(t, ob, got[0])
}
