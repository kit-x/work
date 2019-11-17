package work

import (
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// Enqueuer can enqueue jobs.
type Enqueuer struct {
	client    *Client
	knownJobs *knownJobs
}

func NewEnqueuer(namespace string, opt *redis.Options) *Enqueuer {
	return &Enqueuer{
		client:    NewClient(namespace, opt),
		knownJobs: newKnownJobs(),
	}
}

// Enqueue will enqueue the specified job name and arguments. The args param can be nil if no args ar needed.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com"})
func (enq *Enqueuer) Enqueue(jobName string, args map[string]interface{}) (*Job, error) {
	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: time.Now().Unix(),
		Args:       args,
	}

	if err := enq.client.AddJob(job); err != nil {
		return nil, err
	}
	if err := enq.addToKnownJobs(job.Name); err != nil {
		return nil, err
	}

	return job, nil
}

func (enq *Enqueuer) addToKnownJobs(jobName string) error {
	if !enq.knownJobs.isFresh(jobName) {
		return nil
	}

	return enq.client.addToKnownJobs(jobName)
}

type knownJobs struct {
	cache map[string]int64
	lock  sync.RWMutex
}

func newKnownJobs() *knownJobs {
	return &knownJobs{
		cache: make(map[string]int64),
	}
}

func (jobs *knownJobs) isFresh(jobName string) bool {
	isFresh := true
	now := time.Now().Unix()

	jobs.lock.RLock()
	ts, ok := jobs.cache[jobName]
	jobs.lock.RUnlock()

	if ok && now < ts {
		isFresh = false
	}

	if isFresh {
		jobs.lock.Lock()
		jobs.cache[jobName] = now + 300
		jobs.lock.Unlock()
	}

	return isFresh
}

func (jobs *knownJobs) size() int {
	jobs.lock.RLock()
	defer jobs.lock.RUnlock()
	return len(jobs.cache)
}

func makeIdentifier() string {
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}
