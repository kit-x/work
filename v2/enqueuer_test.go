package work

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/require"
)

func Test_knownJobs_IsFresh(t *testing.T) {
	cache := newKnownJobs()
	cache.cache["old-job"] = time.Now().Unix() - 100

	got := cache.isFresh("old-job")
	require.True(t, got)
	got = cache.isFresh("job")
	require.True(t, got)
	got = cache.isFresh("job")
	require.False(t, got)
}

func TestEnqueuer_Enqueue(t *testing.T) {
	enq := newTestEnqueuer()
	defer enq.cleanup()

	jobName := fakeJobName()
	require.Equal(t, int64(0), enq.client.conn.LLen(enq.client.keys.JobsKey(jobName)).Val())
	require.Equal(t, 0, enq.knownJobs.size())

	// enqueue 1 job
	job, err := enq.Enqueue(jobName, nil)
	require.NoError(t, err)
	require.Equal(t, jobName, job.Name)
	require.Equal(t, int64(1), enq.client.conn.LLen(enq.client.keys.JobsKey(jobName)).Val())
	require.Equal(t, 1, enq.knownJobs.size())
	id1 := job.ID

	// enqueue again
	job, err = enq.Enqueue(jobName, nil)
	require.NoError(t, err)
	require.Equal(t, jobName, job.Name)
	require.Equal(t, int64(2), enq.client.conn.LLen(enq.client.keys.JobsKey(jobName)).Val())
	require.Equal(t, 1, enq.knownJobs.size())
	require.NotEqual(t, id1, job.ID)
}

func newTestEnqueuer(namespace ...string) *Enqueuer {
	var ns string
	if len(namespace) != 0 {
		ns = namespace[0]
	} else {
		ns = callerName(3)
	}

	return NewEnqueuer(ns, &redis.Options{Addr: ":6379"})
}

func (enq *Enqueuer) cleanup() {
	enq.client.cleanup()
}
