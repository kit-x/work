package work

import (
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// WorkerObservation represents the latest observation taken from a worker.
// The observation indicates whether the worker is busy processing a job,
// and if so, information about that job.
type WorkerObservation struct {
	WorkerID string `json:"worker_id"`
	IsBusy   bool   `json:"is_busy"`

	// If IsBusy:
	JobName   string `json:"job_name"`
	JobID     string `json:"job_id"`
	StartedAt int64  `json:"started_at"`
	ArgsJSON  string `json:"args_json"`
	Checkin   string `json:"checkin"`
	CheckinAt int64  `json:"checkin_at"`

	// for test
	heartbeat *WorkerPoolHeartbeat `json:"-"`
}

func newWorkerObservation(hash map[string]string) *WorkerObservation {
	ob := &WorkerObservation{IsBusy: true}
	ob.WorkerID = hash["worker_id"]
	ob.JobName = hash["job_name"]
	ob.JobID = hash["job_id"]
	ob.StartedAt, _ = strconv.ParseInt(hash["started_at"], 10, 64)
	ob.ArgsJSON = hash["args"]
	ob.Checkin = hash["checkin"]
	ob.CheckinAt, _ = strconv.ParseInt(hash["checkin_at"], 10, 64)

	return ob
}

func (wo *WorkerObservation) ToRedis() map[string]interface{} {
	return map[string]interface{}{
		"worker_id":  wo.WorkerID,
		"job_name":   wo.JobName,
		"job_id":     wo.JobID,
		"started_at": wo.StartedAt,
		"args":       wo.ArgsJSON,
		"checkin":    wo.Checkin,
		"checkin_at": wo.CheckinAt,
	}
}

// WorkerObservations returns all of the WorkerObservation's it finds for all worker pools' workers.
func (client *Client) WorkerObservations() ([]*WorkerObservation, error) {
	workerIDs, err := client.getWorkerIDs()
	if err != nil {
		return nil, err
	}

	cmds := make([]*redis.StringStringMapCmd, 0, len(workerIDs))
	fetchOb := func(pipe redis.Pipeliner) error {
		for i := range workerIDs {
			cmd := pipe.HGetAll(client.keys.WorkerObservationKey(workerIDs[i]))
			cmds = append(cmds, cmd)
		}
		return nil
	}
	if _, err = client.conn.Pipelined(fetchOb); err != nil {
		return nil, errors.WithStack(err)
	}

	obs := make([]*WorkerObservation, 0, len(cmds))
	for _, cmd := range cmds {
		result, err := cmd.Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		obs = append(obs, newWorkerObservation(result))
	}

	return obs, nil
}

func (client *Client) SetWorkerObservation(ob *WorkerObservation) error {
	if ob.WorkerID == "" {
		return errors.New("invalid WorkerObservation with empty WorkerID")
	}

	key := client.keys.WorkerObservationKey(ob.WorkerID)
	setOb := func(pipe redis.Pipeliner) error {
		pipe.HMSet(key, ob.ToRedis())
		pipe.Expire(key, time.Hour*24)
		return nil
	}
	if _, err := client.conn.Pipelined(setOb); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (client *Client) DeleteWorkerObservation(workerID string) error {
	return errors.WithStack(client.conn.Del(client.keys.WorkerObservationKey(workerID)).Err())
}

func (client *Client) getWorkerIDs() ([]string, error) {
	beats, err := client.WorkerPoolHeartbeats()
	if err != nil {
		return nil, err
	}

	// TODO: workers count should be set ?
	workerIDs := make([]string, 0, len(beats)*2)
	for _, beat := range beats {
		workerIDs = append(workerIDs, beat.WorkerIDs...)
	}

	return workerIDs, nil
}
