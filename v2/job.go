package work

import (
	"encoding/json"

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
