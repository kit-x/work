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
		conn:    NewConn(opt),
		keys:    newKeys(namespace),
		options: newOption(),
	}
}

// Client implements all of the functionality of the web UI. It can be used to inspect the status of a running cluster and retry dead jobs.
type Client struct {
	conn    *Conn
	keys    *keys
	options *Option
}

// NewClient returns a connection to the Redis Server specified by Options.
func NewConn(opt *redis.Options) *Conn {
	return &Conn{
		Client:     redis.NewClient(opt),
		DeleteJobs: redis.NewScript(redisLuaDeleteSingleCmd),
	}
}

// Conn connection to the Redis Server specified by Options
type Conn struct {
	*redis.Client
	DeleteJobs *redis.Script
}

type Option struct {
	ScheduledJobPageSize int64 `json:"scheduled_job_page_size"`
	RetryJobPageSize     int64 `json:"retry_job_page_size"`
	DeadJobPageSize      int64 `json:"dead_job_page_size"`
}

func newOption() *Option {
	return &Option{
		ScheduledJobPageSize: 20,
		RetryJobPageSize:     20,
		DeadJobPageSize:      20,
	}
}
