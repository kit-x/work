package work

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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

func TestEnqueuer_EnqueueIn(t *testing.T) {
	enq := newTestEnqueuer()
	defer enq.cleanup()

	jobName := fakeJobName()
	require.Equal(t, int64(0), enq.client.conn.ZCard(enq.client.keys.scheduled).Val())
	require.Equal(t, 0, enq.knownJobs.size())

	// enqueue 1 job
	job, err := enq.EnqueueIn(jobName, 100, nil)
	require.NoError(t, err)
	require.Equal(t, jobName, job.Name)
	require.Equal(t, int64(1), enq.client.conn.ZCard(enq.client.keys.scheduled).Val())
	require.Equal(t, 1, enq.knownJobs.size())
	id1 := job.ID

	// enqueue again
	job, err = enq.EnqueueIn(jobName, 100, nil)
	require.NoError(t, err)
	require.Equal(t, jobName, job.Name)
	require.Equal(t, int64(2), enq.client.conn.ZCard(enq.client.keys.scheduled).Val())
	require.Equal(t, 1, enq.knownJobs.size())
	require.NotEqual(t, id1, job.ID)
}

func TestEnqueuer_EnqueueUniqueByKey(t *testing.T) {
	enq := newTestEnqueuer()
	defer enq.cleanup()

	// enqueue unique job by nil keyMap
	jobName := fakeJobName()
	require.Equal(t, 0, enq.knownJobs.size())

	job, err := enq.EnqueueUniqueByKey(jobName, Q{"a": 1, "b": 2}, nil)
	require.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.True(t, job.Unique)
		assert.NotEmpty(t, job.UniqueKey)
	}
	require.Equal(t, 1, enq.knownJobs.size())

	_, err = enq.EnqueueUniqueByKey(jobName, Q{"a": 1, "b": 2}, nil)
	assert.EqualError(t, ErrDupEnqueued, errors.Cause(err).Error())
	require.Equal(t, 1, enq.knownJobs.size())

	_, err = enq.EnqueueUniqueByKey(jobName, Q{"a": 1, "b": 2}, nil)
	assert.EqualError(t, ErrDupEnqueued, errors.Cause(err).Error())

	// enqueue unique job by key condition
	jobName = fakeJobName()
	job, err = enq.EnqueueUniqueByKey(jobName, Q{"a": 1, "b": 2}, Q{"key": 123})
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, 2, enq.knownJobs.size())

	_, err = enq.EnqueueUniqueByKey(jobName, Q{"a": 1, "b": 2}, Q{"key": 123})
	assert.EqualError(t, ErrDupEnqueued, errors.Cause(err).Error())
	require.Equal(t, 2, enq.knownJobs.size())

	job, err = enq.EnqueueUniqueByKey(jobName, Q{"a": 1, "b": 2}, Q{"key": 321})
	require.NoError(t, err)
	require.NotNil(t, job)
}

func TestEnqueuer_EnqueueUniqueInByKey(t *testing.T) {
	enq := newTestEnqueuer()
	defer enq.cleanup()

	// enqueue unique job by nil keyMap
	jobName := fakeJobName()
	require.Equal(t, 0, enq.knownJobs.size())

	job, err := enq.EnqueueUniqueInByKey(jobName, 100, Q{"a": 1, "b": 2}, nil)
	require.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.True(t, job.Unique)
		assert.NotEmpty(t, job.UniqueKey)
		assert.True(t, job.RunAt > time.Now().Unix())
	}
	require.Equal(t, 1, enq.knownJobs.size())

	_, err = enq.EnqueueUniqueInByKey(jobName, 100, Q{"a": 1, "b": 2}, nil)
	assert.EqualError(t, ErrDupEnqueued, errors.Cause(err).Error())
	_, err = enq.EnqueueUniqueInByKey(jobName, 100, Q{"a": 1, "b": 2}, nil)
	assert.EqualError(t, ErrDupEnqueued, errors.Cause(err).Error())

	require.Equal(t, 1, enq.knownJobs.size())

	// enqueue unique job by key condition
	jobName = fakeJobName()
	job, err = enq.EnqueueUniqueInByKey(jobName, 100, Q{"a": 1, "b": 2}, Q{"key": 123})

	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, 2, enq.knownJobs.size())

	job, err = enq.EnqueueUniqueInByKey(jobName, 100, Q{"a": 1, "b": 2}, Q{"key": 123})
	assert.EqualError(t, ErrDupEnqueued, errors.Cause(err).Error())

	job, err = enq.EnqueueUniqueInByKey(jobName, 100, Q{"a": 1, "b": 2}, Q{"key": 321})
	require.NoError(t, err)
	require.NotNil(t, job)
}
