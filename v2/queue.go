package work

import (
	"sort"
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// Queue represents a queue that holds jobs with the same name.
// It indicates their name, count, and latency (in seconds).
// Latency is a measurement of how long ago the next job to be processed was enqueued.
type Queue struct {
	JobName string `json:"job_name"`
	Count   int64  `json:"count"`
	Latency int64  `json:"latency"`

	// for pipeline cmd index
	cmdIndex int
}

// Queues returns the Queue's it finds.
func (client *Client) Queues() ([]*Queue, error) {
	queues, err := client.fetchQueues()
	if err != nil {
		return nil, err
	}

	return client.fetchQueuesLatency(queues)
}

// fetchQueues fetch all queues from known job name set and get queue's size
func (client *Client) fetchQueues() (queues, error) {
	names, err := client.knownJobNames()
	if err != nil {
		return nil, err
	}

	cmds := make([]*redis.IntCmd, 0, len(names))
	fetchQueuesSize := func(pipe redis.Pipeliner) error {
		for i := range names {
			cmd := pipe.LLen(client.keys.JobsKey(names[i]))
			cmds = append(cmds, cmd)
		}
		return nil
	}
	if _, err = client.conn.Pipelined(fetchQueuesSize); err != nil {
		return nil, errors.WithStack(err)
	}
	// cmds and names should be equal
	if len(cmds) != len(names) {
		return nil, errors.New("result is invalid when fetch job size for qs")
	}

	qs := make([]*Queue, 0, len(names))
	for i := range names {
		qs = append(qs, &Queue{
			JobName: names[i],
			Count:   cmds[i].Val(),
		})
	}

	return queues(qs).flushCmdIndex(), nil
}

// fetchQueuesLatency fetch queue's job.EnqueueAt and count latency duration(seconds)
func (client *Client) fetchQueuesLatency(queues queues) ([]*Queue, error) {
	cmds := make([]*redis.StringCmd, 0, len(queues))
	flushQueues := func(pipe redis.Pipeliner) error {
		_ = queues.filterNotEmpty(func(idx int, queue *Queue) error {
			queue.cmdIndex = len(cmds)
			cmd := pipe.LIndex(client.keys.JobsKey(queue.JobName), -1)
			cmds = append(cmds, cmd)
			return nil
		})

		return nil
	}
	if _, err := client.conn.Pipelined(flushQueues); err != nil {
		return nil, errors.WithStack(err)
	}

	now := time.Now().Unix()
	err := queues.filterNotEmpty(func(_ int, queue *Queue) error {
		bs, err := cmds[queue.cmdIndex].Bytes()
		if err != nil {
			return err
		}

		job, err := newJob(bs)
		if err != nil {
			return err
		}

		queue.Latency = now - job.EnqueuedAt

		return nil
	})
	if err != nil {
		return nil, err
	}

	return queues, nil
}

func (client *Client) knownJobNames() ([]string, error) {
	names, err := client.conn.SMembers(client.keys.KnownJobsKey()).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sort.Strings(names)
	return names, nil
}

type queues []*Queue

func (qs queues) filterNotEmpty(f func(idx int, queue *Queue) error) error {
	for i := range qs {
		if qs[i].Count == 0 {
			continue
		}

		if err := f(i, qs[i]); err != nil {
			return err
		}
	}

	return nil
}

func (qs queues) flushCmdIndex() queues {
	for i := range qs {
		qs[i].cmdIndex = -1
	}

	return qs
}
