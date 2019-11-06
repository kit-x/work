package v2

import (
	"strconv"
	"strings"
)

// WorkerPoolHeartbeat represents the heartbeat from a worker pool.
// WorkerPool's write a heartbeat every 5 seconds so we know they're alive and includes config information.
type WorkerPoolHeartbeat struct {
	WorkerPoolID string   `json:"worker_pool_id"`
	StartedAt    int64    `json:"started_at"`
	HeartbeatAt  int64    `json:"heartbeat_at"`
	JobNames     []string `json:"job_names"`
	Concurrency  uint     `json:"concurrency"`
	Host         string   `json:"host"`
	Pid          int      `json:"pid"`
	WorkerIDs    []string `json:"worker_ids"`
}

func newWorkerPoolHeartbeat(hash map[string]string) *WorkerPoolHeartbeat {
	beat := &WorkerPoolHeartbeat{
		JobNames:  make([]string, 0),
		WorkerIDs: make([]string, 0),
	}
	beat.WorkerPoolID = hash["worker_pool_id"]
	beat.StartedAt, _ = strconv.ParseInt(hash["started_at"], 10, 64)
	beat.HeartbeatAt, _ = strconv.ParseInt(hash["heartbeat_at"], 10, 64)
	beat.JobNames = strings.Split(hash["job_names"], ",")
	concurrency, _ := strconv.ParseUint(hash["concurrency"], 10, 64)
	beat.Concurrency = uint(concurrency)
	beat.Host = hash["host"]
	beat.Pid, _ = strconv.Atoi(hash["pid"])
	beat.WorkerIDs = strings.Split(hash["worker_ids"], ",")

	return beat
}

func (wph *WorkerPoolHeartbeat) ToRedis() map[string]interface{} {
	hash := map[string]interface{}{
		"worker_pool_id": wph.WorkerPoolID,
		"started_at":     wph.StartedAt,
		"heartbeat_at":   wph.HeartbeatAt,
		"job_names":      strings.Join(wph.JobNames, ","),
		"concurrency":    wph.Concurrency,
		"host":           wph.Host,
		"pid":            wph.Pid,
		"worker_ids":     strings.Join(wph.WorkerIDs, ","),
	}

	return hash
}
