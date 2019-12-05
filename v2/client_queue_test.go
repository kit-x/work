package work

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Queues(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	// mock 2 job with items
	jobs := client.mockJobs()
	queues, err := client.Queues()
	require.NoError(t, err)
	require.Equal(t, len(queues), len(jobs))
	for _, queue := range queues {
		found := false
		for _, job := range jobs {
			if job.Name == queue.JobName {
				found = true
				assert.Equal(t, job.Name, queue.JobName)
				assert.Equal(t, int64(1), queue.Count)
				assert.True(t, queue.Latency > 0)
				assert.True(t, queue.cmdIndex != -1)
			}
		}

		if !found {
			t.Fatalf("queue's jobname %s not found in jobs", queue.JobName)
		}
	}

	// cleanup data
	client.cleanup()

	// mock 1 empty job
	name := "job-empty"
	client.mockKnownJobNames(name)
	queues, err = client.Queues()
	require.NoError(t, err)
	if assert.Equal(t, 1, len(queues)) {
		assert.Equal(t, name, queues[0].JobName)
		assert.Equal(t, int64(0), queues[0].Count)
		assert.True(t, queues[0].Latency == 0)
	}
}
