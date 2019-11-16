package work

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/zhaolion/gofaker"
)

// cleanup when in testing. it should only used in test
func (client *Client) cleanup() {
	keys, err := client.conn.Keys(fmt.Sprintf("%s*", client.keys.NameSpace())).Result()
	if err != nil {
		panic(err)
	}

	if len(keys) == 0 {
		return
	}

	if err := client.conn.Del(keys...).Err(); err != nil {
		panic(err)
	}
}

func (client *Client) mockWorkerPoolHeartbeat() *WorkerPoolHeartbeat {
	heartbeat := fakeWorkerPoolHeartbeat()

	must(func() error {
		return client.conn.SAdd(client.keys.WorkerPoolsKey(), heartbeat.WorkerPoolID).Err()
	})
	must(func() error {
		return client.conn.HMSet(client.keys.HeartbeatKey(heartbeat.WorkerPoolID), heartbeat.ToRedis()).Err()
	})

	return heartbeat
}

func (client *Client) mockWorkerPoolIDs(ids ...string) {
	must(func() error {
		return client.conn.SAdd(client.keys.WorkerPoolsKey(), ids).Err()
	})
}

func (client *Client) mockWorkerObservation() *WorkerObservation {
	heartbeat := client.mockWorkerPoolHeartbeat()

	ob := fakeWorkerObservation()
	ob.heartbeat = heartbeat
	ob.WorkerID = heartbeat.WorkerIDs[0]
	ob.JobName = heartbeat.JobNames[0]
	must(func() error {
		return client.conn.HMSet(client.keys.WorkerObservationKey(ob.WorkerID), ob.ToRedis()).Err()
	})

	return ob
}

func (client *Client) mockKnownJobNames(jobs ...string) {
	must(func() error {
		return client.conn.SAdd(client.keys.KnownJobsKey(), jobs).Err()
	})
}

func (client *Client) mockJobs(count ...int) jobs {
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

	client.mockKnownJobNames(jobs.Names()...)

	for _, job := range jobs {
		must(func() error {
			return client.conn.LPush(client.keys.JobsKey(job.Name), job).Err()
		})
	}

	return jobs
}

func must(f func() error) {
	if err := f(); err != nil {
		panic(errors.WithStack(err))
	}
}
