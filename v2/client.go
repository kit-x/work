package work

import (
	"fmt"
	"sort"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// ErrNotDeleted is returned by functions that delete jobs to indicate that although the redis commands were successful,
// no object was actually deleted by those commmands.
var ErrNotDeleted = fmt.Errorf("nothing deleted")

// ErrNotRetried is returned by functions that retry jobs to indicate that although the redis commands were successful,
// no object was actually retried by those commmands.
var ErrNotRetried = fmt.Errorf("nothing retried")

// NewClient creates a new Client with the specified redis namespace and connection pool.
func NewClient(namespace string, opt *redis.Options) *Client {
	return &Client{
		conn: NewConn(opt),
		keys: newKeys(namespace),
	}
}

// Client implements all of the functionality of the web UI. It can be used to inspect the status of a running cluster and retry dead jobs.
type Client struct {
	conn *Conn
	keys *keys
}

// NewClient returns a connection to the Redis Server specified by Options.
func NewConn(opt *redis.Options) *Conn {
	return &Conn{
		Client: redis.NewClient(opt),
	}
}

// Conn connection to the Redis Server specified by Options
type Conn struct {
	*redis.Client
}

func newKeys(namespace string) *keys {
	l := len(namespace)
	if (l > 0) && (namespace[l-1] != ':') {
		namespace = namespace + ":"
	}

	return &keys{
		namespace:   namespace,
		workerPools: namespace + "worker_pools",
	}
}

type keys struct {
	namespace   string
	workerPools string
}

func (ks keys) NameSpace() string {
	return ks.namespace
}

func (ks keys) WorkerPoolsKey() string {
	return ks.workerPools
}

func (ks keys) HeartbeatKey(id string) string {
	return fmt.Sprintf("%s:%s", ks.workerPools, id)
}

// WorkerPoolHeartbeats queries Redis and returns all WorkerPoolHeartbeat's it finds (even for those worker pools which don't have a current heartbeat).
func (c *Client) WorkerPoolHeartbeats() ([]*WorkerPoolHeartbeat, error) {
	// fetch worker pool ids
	workerPoolIDs, err := c.getWorkerPoolIDs()
	if err != nil {
		return nil, err
	}

	beats := make([]*WorkerPoolHeartbeat, 0, len(workerPoolIDs))

	// send heart beat for each pool
	cmds := make([]*redis.StringStringMapCmd, 0, len(workerPoolIDs))
	sendHeartBeat := func(pipe redis.Pipeliner) error {
		for i := range workerPoolIDs {
			cmd := pipe.HGetAll(c.keys.HeartbeatKey(workerPoolIDs[i]))
			cmds = append(cmds, cmd)
		}

		return nil
	}
	if _, err = c.conn.Pipelined(sendHeartBeat); err != nil {
		return nil, errors.WithStack(err)
	}

	for _, cmd := range cmds {
		result, err := cmd.Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		beats = append(beats, newWorkerPoolHeartbeat(result))
	}

	return beats, nil
}

// cleanup when in testing. it should only used in test
func (c *Client) cleanup() {
	keys, err := c.conn.Keys(fmt.Sprintf("%s*", c.keys.NameSpace())).Result()
	if err != nil {
		panic(err)
	}

	if len(keys) == 0 {
		return
	}

	if err := c.conn.Del(keys...).Err(); err != nil {
		panic(err)
	}
}

func (c *Client) setWorkerPoolIDs(ids ...string) error {
	return c.conn.SAdd(c.keys.WorkerPoolsKey(), ids).Err()
}

func (c *Client) getWorkerPoolIDs() ([]string, error) {
	workerPoolIDs, err := c.conn.SMembers(c.keys.WorkerPoolsKey()).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sort.Strings(workerPoolIDs)

	return workerPoolIDs, nil
}

func (c *Client) setWorkerPoolHeartbeat(heartbeat *WorkerPoolHeartbeat) error {
	return c.conn.HMSet(c.keys.HeartbeatKey(heartbeat.WorkerPoolID), heartbeat.ToRedis()).Err()
}
