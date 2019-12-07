package work

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_requeue(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	jobs := client.mockScheduledJobs(2)
	assert.Equal(t, int64(2), client.conn.ZCard(client.keys.ScheduledKey()).Val())
	jobNames := make([]string, 0)
	for _, job := range jobs {
		jobNames = append(jobNames, job.Name)
		assert.Equal(t, int64(0), client.conn.LLen(client.keys.JobsKey(job.Name)).Val())
	}

	remain, err := client.requeue(client.keys.ScheduledKey(), jobNames)
	require.NoError(t, err)
	assert.True(t, remain)
	remain, err = client.requeue(client.keys.ScheduledKey(), jobNames)
	require.NoError(t, err)
	assert.True(t, remain)
	remain, err = client.requeue(client.keys.ScheduledKey(), jobNames)
	require.NoError(t, err)
	assert.False(t, remain)
	for _, job := range jobs {
		assert.Equal(t, int64(1), client.conn.LLen(client.keys.JobsKey(job.Name)).Val())
	}
}
