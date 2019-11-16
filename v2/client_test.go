package work

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/zhaolion/gofaker"
)

// cleanup when in testing. it should only used in test
func (c *Client) cleanup() {
	keys, err := c.conn.Keys(fmt.Sprintf("%s*", c.keys.NameSpace())).Result()
	if err != nil {
		panic(err)
	}

	if len(keys) == 0 {
		return
	}

	if err := c.conn.Del(keys...).Err(); err != nil {
		panic(err)
	}
}

func (c *Client) mockWorkerPoolHeartbeat() *WorkerPoolHeartbeat {
	heartbeat := fakeWorkerPoolHeartbeat()

	must(func() error {
		return c.conn.SAdd(c.keys.WorkerPoolsKey(), heartbeat.WorkerPoolID).Err()
	})
	must(func() error {
		return c.conn.HMSet(c.keys.HeartbeatKey(heartbeat.WorkerPoolID), heartbeat.ToRedis()).Err()
	})

	return heartbeat
}

func (c *Client) mockWorkerPoolIDs(ids ...string) {
	must(func() error {
		return c.conn.SAdd(c.keys.WorkerPoolsKey(), ids).Err()
	})
}

func (c *Client) mockWorkerObservation() *WorkerObservation {
	heartbeat := c.mockWorkerPoolHeartbeat()

	ob := fakeWorkerObservation()
	ob.heartbeat = heartbeat
	ob.WorkerID = heartbeat.WorkerIDs[0]
	ob.JobName = heartbeat.JobNames[0]
	must(func() error {
		return c.conn.HMSet(c.keys.WorkerObservationKey(ob.WorkerID), ob.ToRedis()).Err()
	})

	return ob
}

func (c *Client) mockKnownJobNames(jobs ...string) {
	must(func() error {
		return c.conn.SAdd(c.keys.KnownJobsKey(), jobs).Err()
	})
}

func (c *Client) mockJobs(count ...int) jobs {
	size := 2
	if len(count) != 0 {
		size = count[0]
	}

	jobs := make(jobs, 0)
	for i := 0; i < size; i++ {
		jobs = append(jobs, &Job{
			ID:         gofaker.Alpha(4),
			Name:       "job" + gofaker.Alpha(4),
			EnqueuedAt: time.Now().Unix() - 100,
		})
	}

	c.mockKnownJobNames(jobs.Names()...)

	for _, job := range jobs {
		must(func() error {
			return c.conn.LPush(c.keys.JobsKey(job.Name), job).Err()
		})
	}

	return jobs
}

func must(f func() error) {
	if err := f(); err != nil {
		panic(errors.WithStack(err))
	}
}
