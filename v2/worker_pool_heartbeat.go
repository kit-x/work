package work

import (
	"sort"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
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
	return map[string]interface{}{
		"worker_pool_id": wph.WorkerPoolID,
		"started_at":     wph.StartedAt,
		"heartbeat_at":   wph.HeartbeatAt,
		"job_names":      strings.Join(wph.JobNames, ","),
		"concurrency":    wph.Concurrency,
		"host":           wph.Host,
		"pid":            wph.Pid,
		"worker_ids":     strings.Join(wph.WorkerIDs, ","),
	}
}

// WorkerPoolHeartbeats queries Redis and returns all WorkerPoolHeartbeat's it finds (even for those worker pools which don't have a current heartbeat).
func (client *Client) WorkerPoolHeartbeats() ([]*WorkerPoolHeartbeat, error) {
	// fetch worker pool ids
	workerPoolIDs, err := client.getWorkerPoolIDs()
	if err != nil {
		return nil, err
	}

	// send heart beat for each pool
	cmds := make([]*redis.StringStringMapCmd, 0, len(workerPoolIDs))
	sendHeartBeat := func(pipe redis.Pipeliner) error {
		for i := range workerPoolIDs {
			cmd := pipe.HGetAll(client.keys.HeartbeatKey(workerPoolIDs[i]))
			cmds = append(cmds, cmd)
		}

		return nil
	}
	if _, err = client.conn.Pipelined(sendHeartBeat); err != nil {
		return nil, errors.WithStack(err)
	}

	beats := make([]*WorkerPoolHeartbeat, 0, len(workerPoolIDs))
	for _, cmd := range cmds {
		result, err := cmd.Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		beats = append(beats, newWorkerPoolHeartbeat(result))
	}

	return beats, nil
}

func (client *Client) getWorkerPoolIDs() ([]string, error) {
	workerPoolIDs, err := client.conn.SMembers(client.keys.WorkerPoolsKey()).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sort.Strings(workerPoolIDs)

	return workerPoolIDs, nil
}
