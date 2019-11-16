package work

import (
	"encoding/json"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// Job represents a job.
type Job struct {
	// Inputs when making a new job

	ID         string                 `json:"id"`
	Name       string                 `json:"name,omitempty"`
	EnqueuedAt int64                  `json:"t"`
	Args       map[string]interface{} `json:"args"`

	// Inputs when retrying

	Fails    int64  `json:"fails,omitempty"` // number of times this job has failed
	LastErr  string `json:"err,omitempty"`
	FailedAt int64  `json:"failed_at,omitempty"`
}

func newJob(rawBytes []byte) (*Job, error) {
	var job Job
	err := json.Unmarshal(rawBytes, &job)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &job, nil
}

func (job *Job) MarshalBinary() ([]byte, error) {
	return json.Marshal(job)
}

type jobs []*Job

func (jobs jobs) Names() []string {
	names := make([]string, 0, len(jobs))
	for i := range jobs {
		names = append(names, jobs[i].Name)
	}

	return names
}

// ScheduledJob represents a job in the scheduled queue.
type ScheduledJob struct {
	*Job

	RunAt int64 `json:"run_at"`
}

func newScheduledJob(rawBytes []byte) (*ScheduledJob, error) {
	j, err := newJob(rawBytes)
	if err != nil {
		return nil, err
	}

	return &ScheduledJob{Job: j}, nil
}

func (job *ScheduledJob) MarshalBinary() ([]byte, error) {
	return json.Marshal(job)
}

// ScheduledJobs returns a list of ScheduledJob's.
// The page param is 1-based; each page is 20 items.
// The total number of items (not pages) in the list of scheduled jobs is also returned.
func (client *Client) ScheduledJobs(page int64) ([]*ScheduledJob, int64, error) {
	scoreJobs, total, err := client.jobs(client.keys.scheduled, page, client.options.ScheduledJobPageSize)
	if err != nil {
		return nil, 0, err
	}

	jobs := make([]*ScheduledJob, 0, len(scoreJobs))
	for i := range scoreJobs {
		job, err := newScheduledJob(scoreJobs[i].Bytes)
		if err != nil {
			return nil, 0, err
		}
		job.RunAt = scoreJobs[i].Score
		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// RetryJob represents a job in the retry queue.
type RetryJob struct {
	*Job

	RetryAt int64 `json:"retry_at"`
}

func newRetryJob(rawBytes []byte) (*RetryJob, error) {
	j, err := newJob(rawBytes)
	if err != nil {
		return nil, err
	}

	return &RetryJob{Job: j}, nil
}

func (job *RetryJob) MarshalBinary() ([]byte, error) {
	return json.Marshal(job)
}

// RetryJobs returns a list of RetryJob's.
// The page param is 1-based; each page is 20 items.
// The total number of items (not pages) in the list of retry jobs is also returned.
func (client *Client) RetryJobs(page int64) ([]*RetryJob, int64, error) {
	scoreJobs, total, err := client.jobs(client.keys.retry, page, client.options.RetryJobPageSize)
	if err != nil {
		return nil, 0, err
	}

	jobs := make([]*RetryJob, 0, len(scoreJobs))
	for i := range scoreJobs {
		job, err := newRetryJob(scoreJobs[i].Bytes)
		if err != nil {
			return nil, 0, err
		}
		job.RetryAt = scoreJobs[i].Score
		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// DeadJob represents a job in the dead queue.
type DeadJob struct {
	*Job

	DiedAt int64 `json:"died_at"`
}

func newDeadJob(rawBytes []byte) (*DeadJob, error) {
	j, err := newJob(rawBytes)
	if err != nil {
		return nil, err
	}

	return &DeadJob{Job: j}, nil
}

func (job *DeadJob) MarshalBinary() ([]byte, error) {
	return json.Marshal(job)
}

// DeadJobs returns a list of DeadJob's.
// The page param is 1-based; each page is 20 items.
// The total number of items (not pages) in the list of dead jobs is also returned.
func (client *Client) DeadJobs(page int64) ([]*DeadJob, int64, error) {
	scoreJobs, total, err := client.jobs(client.keys.dead, page, client.options.RetryJobPageSize)
	if err != nil {
		return nil, 0, err
	}

	jobs := make([]*DeadJob, 0, len(scoreJobs))
	for i := range scoreJobs {
		job, err := newDeadJob(scoreJobs[i].Bytes)
		if err != nil {
			return nil, 0, err
		}
		job.DiedAt = scoreJobs[i].Score
		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// DeleteDeadJob deletes a dead job from Redis.
func (client *Client) DeleteDeadJob(diedAt int64, jobID string) error {
	ok, _, err := client.deleteJobs(client.keys.dead, diedAt, jobID)
	if err != nil {
		return err
	}
	if !ok {
		return errors.WithStack(ErrNotDeleted)
	}

	return nil
}

type scoreJob struct {
	Bytes []byte
	Score int64
}

func (client *Client) jobs(key string, page, limit int64) (jobs []*scoreJob, total int64, e error) {
	// fetch jobs and limit total in pipeline
	var (
		jobsCmd  *redis.ZSliceCmd
		countCmd *redis.IntCmd
	)
	paginateScheduledJobs := func(pipe redis.Pipeliner) error {
		opt := redis.ZRangeBy{
			Max: "+inf", Min: "-inf",
			Offset: offset(page, limit), Count: limit,
		}
		jobsCmd = pipe.ZRangeByScoreWithScores(key, opt)
		countCmd = pipe.ZCard(key)

		return nil
	}
	if _, err := client.conn.Pipelined(paginateScheduledJobs); err != nil {
		return nil, 0, errors.WithStack(err)
	}
	results, err := jobsCmd.Result()
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	total, err = countCmd.Result()
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}

	jobs = make([]*scoreJob, 0, len(results))
	for i := range results {
		s, ok := results[i].Member.(string)
		if !ok {
			return nil, 0, errors.New("zrangebyscore member should be string")
		}

		jobs = append(jobs, &scoreJob{
			Bytes: []byte(s),
			Score: int64(results[i].Score),
		})
	}

	return jobs, total, nil
}

func offset(page, limit int64) int64 {
	if page <= 0 {
		page = 1
	}

	return (page - 1) * limit
}

func (client *Client) deleteJobs(key string, score int64, jobID string) (bool, []byte, error) {
	result, err := client.conn.DeleteJobs.Run(
		client.conn,
		[]string{client.keys.dead},
		score, jobID,
	).Result()
	if err != nil {
		return false, nil, errors.WithStack(err)
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return false, nil, errors.Errorf("need 2 elements back from redis command, but got %+v", result)
	}

	count, err := Int64(values[0])
	if err != nil {
		return false, nil, errors.WithStack(err)
	}

	str, err := String(values[1])
	if err != nil {
		return false, nil, errors.WithStack(err)
	}

	return count > 0, []byte(str), nil
}
