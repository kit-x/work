package work

import (
	"encoding/json"

	"github.com/pkg/errors"
)

// Q is a shortcut to easily specify arguments for jobs when enqueueing them.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com", "track": true})
type Q map[string]interface{}

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
