package work

import (
	"fmt"

	"github.com/go-redis/redis"
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
	return fmt.Sprintf("%s:%s:%s", ks.namespace, ks.workerPools, id)
}

func (ks keys) WorkerObservationKey(id string) string {
	return fmt.Sprintf("%s:worker:%s", ks.namespace, id)
}
