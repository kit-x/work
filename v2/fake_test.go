package work

import (
	"strconv"
	"time"
)

func fakeWorkerPoolHeartbeat() *WorkerPoolHeartbeat {
	return &WorkerPoolHeartbeat{
		WorkerPoolID: "a",
		StartedAt:    time.Now().Unix(),
		HeartbeatAt:  time.Now().Unix(),
		JobNames:     []string{"job-a", "job-b", "job-c"},
		Concurrency:  1,
		Host:         "host",
		Pid:          1,
		WorkerIDs:    []string{"worker-a", "worker-b", "worker-c"},
	}
}

func fakeWorkerObservation() *WorkerObservation {
	return &WorkerObservation{
		WorkerID:  "a",
		IsBusy:    true,
		JobName:   "job-a",
		JobID:     strconv.FormatInt(time.Now().Unix(), 10),
		StartedAt: time.Now().Unix(),
		ArgsJSON:  "{}",
		Checkin:   "checkin",
		CheckinAt: time.Now().Unix(),
	}
}
