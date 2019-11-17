package work

import (
	"encoding/json"
	"time"

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
	Unique     bool                   `json:"unique,omitempty"`
	UniqueKey  string                 `json:"unique_key,omitempty"`

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

// AddJob will enqueue the job into specified list.
func (client *Client) AddJob(job *Job) error {
	if err := client.conn.LPush(client.keys.JobsKey(job.Name), job).Err(); err != nil {
		return errors.WithStack(err)
	}

	return nil
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

func (client *Client) AddScheduledJob(job *ScheduledJob) error {
	if err := client.conn.ZAdd(client.keys.scheduled, redis.Z{Member: job, Score: float64(job.RunAt)}).Err(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// ScheduledJobs returns a list of ScheduledJob's.
// The page param is 1-based; each page is 20 items.
// The total number of items (not pages) in the list of scheduled jobs is also returned.
func (client *Client) ScheduledJobs(page int) ([]*ScheduledJob, int64, error) {
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

// DeleteScheduledJob deletes a job in the scheduled queue.
func (client *Client) DeleteScheduledJob(scheduledFor int64, jobID string) error {
	ok, bytes, err := client.deleteJobs(client.keys.scheduled, scheduledFor, jobID)
	if err != nil {
		return err
	}
	if !ok {
		return errors.WithStack(ErrNotDeleted)
	}
	// maybe some error happened ?
	if len(bytes) == 0 {
		return nil
	}

	// If we get a job back, parse it and see if it's a unique job.
	job, err := newJob(bytes)
	if err != nil {
		return err
	}

	// If it is, we need to delete the unique key.
	if job.Unique {
		uniqueKey, err := client.keys.UniqueJobKey(job.Name, job.Args)
		if err != nil {
			return err
		}
		if err := client.conn.Del(uniqueKey).Err(); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
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
func (client *Client) RetryJobs(page int) ([]*RetryJob, int64, error) {
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

func (client *Client) DeleteRetryJob(retryAt int64, jobID string) error {
	ok, _, err := client.deleteJobs(client.keys.retry, retryAt, jobID)
	if err != nil {
		return err
	}
	if !ok {
		return errors.WithStack(ErrNotDeleted)
	}

	return nil
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
func (client *Client) DeadJobs(page int) ([]*DeadJob, int64, error) {
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

// DeleteAllDeadJobs deletes all dead jobs.
func (client *Client) DeleteAllDeadJobs() error {
	if err := client.conn.Del(client.keys.dead).Err(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// RetryDeadJob retries a dead job.
// The job will be re-queued on the normal work queue for eventual processing by a worker.
func (client *Client) RetryDeadJob(diedAt int64, jobID string) error {
	// why not using Queues(), because client.Queues is too expensive
	jobNames, err := client.knownJobNames()
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(jobNames)+1)
	// KEY[1]
	keys = append(keys, client.keys.dead)
	// KEY[2, 3, ...]
	for i := range jobNames {
		keys = append(keys, client.keys.JobsKey(jobNames[i]))
	}

	result, err := client.script.RetryDeadJob.Run(
		client.conn,
		keys,
		client.keys.jobsPrefix,
		time.Now().Unix(),
		diedAt,
		jobID,
	).Result()
	if err != nil {
		return errors.WithStack(err)
	}

	count, err := Int64(result)
	if err != nil {
		return errors.WithStack(err)
	}
	if count == 0 {
		return ErrNotRetried
	}

	return nil
}

// RetryAllDeadJobs requeues all dead jobs.
// In other words, it puts them all back on the normal work queue for workers to pull from and process.
func (client *Client) RetryAllDeadJobs(limit ...int) error {
	// why not using Queues(), because client.Queues is too expensive
	jobNames, err := client.knownJobNames()
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(jobNames)+1)
	// KEY[1]
	keys = append(keys, client.keys.dead)
	// KEY[2, 3, ...]
	for i := range jobNames {
		keys = append(keys, client.keys.JobsKey(jobNames[i]))
	}

	// Cap iterations for safety (which could reprocess 1k*1k jobs).
	// This is conceptually an infinite loop but let's be careful.
	max := defaultNum(client.options.RequeueAllMaxPage, limit...)
	for i := 0; i < max; i++ {
		result, err := client.script.RequeueAllDead.Run(
			client.conn,
			keys,
			client.keys.jobsPrefix,
			time.Now().Unix(),
			client.options.RequeueAllPageSize,
		).Result()
		if err != nil {
			return errors.WithStack(err)
		}

		count, err := Int64(result)
		if err != nil {
			return errors.WithStack(err)
		}
		if count == 0 {
			break
		}
	}

	return nil
}

type scoreJob struct {
	Bytes []byte
	Score int64
}

func (client *Client) jobs(key string, page, limit int) (jobs []*scoreJob, total int64, e error) {
	// fetch jobs and limit total in pipeline
	var (
		jobsCmd  *redis.ZSliceCmd
		countCmd *redis.IntCmd
	)
	paginateScheduledJobs := func(pipe redis.Pipeliner) error {
		opt := redis.ZRangeBy{
			Max: "+inf", Min: "-inf",
			Offset: offset(page, limit), Count: int64(limit),
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

func offset(page, limit int) int64 {
	if page <= 0 {
		page = 1
	}

	return int64((page - 1) * limit)
}

func (client *Client) deleteJobs(key string, score int64, jobID string) (bool, []byte, error) {
	result, err := client.script.DeleteJobAtZSet.Run(
		client.conn,
		[]string{key},
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

func (client *Client) addToKnownJobs(jobName ...string) error {
	if err := client.conn.SAdd(client.keys.KnownJobsKey(), jobName).Err(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
