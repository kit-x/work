package work

import (
	"strconv"
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
