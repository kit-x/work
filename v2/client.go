package work

import (
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// ErrNotDeleted is returned by functions that delete jobs to indicate that although the redis commands were successful,
// no object was actually deleted by those commmands.
var ErrNotDeleted = errors.New("nothing deleted")

// ErrNotRetried is returned by functions that retry jobs to indicate that although the redis commands were successful,
// no object was actually retried by those commmands.
var ErrNotRetried = errors.New("nothing retried")

// ErrDupEnqueued is returned by functions that enqueue duplicate job that already enqueued with the same name and key
var ErrDupEnqueued = errors.New("enqueue duplicate unique job")

// NewClient creates a new Client with the specified redis namespace and connection pool.
func NewClient(namespace string, opt *redis.Options) *Client {
	return &Client{
		conn:    NewConn(opt),
		script:  newScript(),
		keys:    newKeys(namespace),
		options: newOption(),
	}
}

// Client implements all of the functionality of the web UI. It can be used to inspect the status of a running cluster and retry dead jobs.
type Client struct {
	conn    *Conn
	script  *Script
	keys    *Keys
	options *Option
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

type Option struct {
	ScheduledJobPageSize int `json:"scheduled_job_page_size"`
	RetryJobPageSize     int `json:"retry_job_page_size"`
	DeadJobPageSize      int `json:"dead_job_page_size"`
	RequeueAllPageSize   int `json:"requeue_all_page_size"`
	RequeueAllMaxPage    int `json:"requeue_all_page_size"`
}

func newOption() *Option {
	return &Option{
		ScheduledJobPageSize: 20,
		RetryJobPageSize:     20,
		DeadJobPageSize:      20,
		RequeueAllPageSize:   1000,
		RequeueAllMaxPage:    1000,
	}
}
