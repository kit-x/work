package work

import (
	"strconv"
	"testing"
	"time"

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

func TestClient_SetWorkerObservation(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	ob := fakeWorkerObservation()
	err := client.SetWorkerObservation(ob)
	if err != nil {
		t.Fatalf("should not received err, but got %+v", err)
	}

	got := client.conn.HGetAll(client.keys.WorkerObservationKey(ob.WorkerID)).Val()
	assert.Equal(t, ob.WorkerID, got["worker_id"])
	assert.Equal(t, ob.JobName, got["job_name"])
	assert.Equal(t, ob.JobID, got["job_id"])
	assert.Equal(t, strconv.FormatInt(ob.StartedAt, 10), got["started_at"])
	assert.Equal(t, ob.ArgsJSON, got["args"])
	assert.Equal(t, ob.Checkin, got["checkin"])
	assert.Equal(t, strconv.FormatInt(ob.CheckinAt, 10), got["checkin_at"])

	ttl := client.conn.TTL(client.keys.WorkerObservationKey(ob.WorkerID)).Val()
	assert.True(t, ttl > 0 && ttl <= time.Hour*24)

	ob.WorkerID = ""
	err = client.SetWorkerObservation(ob)
	assert.EqualError(t, err, "invalid WorkerObservation with empty WorkerID")
}

func TestClient_DeleteWorkerObservation(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	ob := client.mockWorkerObservation()
	num := client.conn.Exists(client.keys.WorkerObservationKey(ob.WorkerID)).Val()
	assert.True(t, num > 0)

	err := client.DeleteWorkerObservation(ob.WorkerID)
	if assert.NoError(t, err) {
		num = client.conn.Exists(client.keys.WorkerObservationKey(ob.WorkerID)).Val()
		assert.True(t, num == 0)
	}
}
