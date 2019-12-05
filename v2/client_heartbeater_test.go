package work

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient_WorkerPoolHeartbeats(t *testing.T) {
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

func TestClient_SendHeartbeat(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	beat := fakeWorkerPoolHeartbeat()
	assert.Equal(t, int64(0), client.conn.SCard(client.keys.WorkerPoolsKey()).Val())
	assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "heartbeat_at").Val())
	assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "started_at").Val())
	assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "job_names").Val())
	assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "concurrency").Val())
	assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "worker_ids").Val())
	assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "host").Val())
	assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "pid").Val())

	err := client.SendHeartbeat(beat)
	if assert.NoError(t, err) {
		assert.Equal(t, int64(1), client.conn.SCard(client.keys.WorkerPoolsKey()).Val())
		assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "heartbeat_at").Val())
		assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "started_at").Val())
		assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "job_names").Val())
		assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "concurrency").Val())
		assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "worker_ids").Val())
		assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "host").Val())
		assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "pid").Val())
	}
}

func TestClient_RemoveWorkerPoolHeartbeat(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	beat := client.mockWorkerPoolHeartbeat()
	assert.Equal(t, int64(1), client.conn.SCard(client.keys.WorkerPoolsKey()).Val())
	assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "heartbeat_at").Val())
	assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "started_at").Val())
	assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "job_names").Val())
	assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "concurrency").Val())
	assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "worker_ids").Val())
	assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "host").Val())
	assert.True(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "pid").Val())

	err := client.RemoveWorkerPoolHeartbeat(beat.WorkerPoolID)
	if assert.NoError(t, err) {
		assert.Equal(t, int64(0), client.conn.SCard(client.keys.WorkerPoolsKey()).Val())
		assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "heartbeat_at").Val())
		assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "started_at").Val())
		assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "job_names").Val())
		assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "concurrency").Val())
		assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "worker_ids").Val())
		assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "host").Val())
		assert.False(t, client.conn.HExists(client.keys.HeartbeatKey(beat.WorkerPoolID), "pid").Val())
	}
}
