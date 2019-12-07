package work

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// requeue: requeue zset's job into job's queue one by one
// if we don't know the jobName of a job, we'll put it in dead.
//
// requeueKey: should be redis zset key
//
// return:
// * bool remain: still remain jobs in requeueKey
// * error err
func (client *Client) requeue(requeueKey string, jobNames []string) (bool, error) {
	keys := make([]string, 0, 2+len(jobNames))
	// KEY[1]
	keys = append(keys, requeueKey)
	// KEY[2]
	keys = append(keys, client.keys.DeadKey())
	// KEY[3, 4, ...]
	for i := range jobNames {
		keys = append(keys, client.keys.JobsKey(jobNames[i]))
	}
	result, err := client.script.Requeue.Run(
		client.conn,
		keys,                   // KEYS
		client.keys.jobsPrefix, // ARGV[1]
		time.Now().Unix(),      // ARGV[2] -- NOTE: We're going to change this one on every call
	).Result()
	if err != nil {
		// zset is empty, ignore this err
		if err == redis.Nil {
			return false, nil
		}

		return false, errors.WithStack(err)
	}
	if result == "ok" || result == "dead" {
		return true, nil
	}
	return false, nil
}
