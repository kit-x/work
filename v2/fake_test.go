package work

import (
	"strconv"
	"time"

	"github.com/zhaolion/gofaker"
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

func fakeScheduledJob() *ScheduledJob {
	return &ScheduledJob{
		Job:   fakeJob(),
		RunAt: time.Now().Unix() + 100,
	}
}

func fakeJob() *Job {
	return &Job{
		ID:         gofaker.Alpha(4),
		Name:       fakeJobName(),
		EnqueuedAt: time.Now().Unix(),
		Args:       make(map[string]interface{}),
	}
}

func fakeJobName() string {
	return "job" + gofaker.Alpha(4)
}
