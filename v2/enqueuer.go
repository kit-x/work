package work

import (
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
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

func (enq *Enqueuer) EnqueueIn(jobName string, secondsFromNow int64, args map[string]interface{}) (*ScheduledJob, error) {
	now := time.Now().Unix()
	job := &ScheduledJob{
		RunAt: now + secondsFromNow,
		Job: &Job{
			Name:       jobName,
			ID:         makeIdentifier(),
			EnqueuedAt: now,
			Args:       args,
		},
	}

	if err := enq.client.AddScheduledJob(job); err != nil {
		return nil, err
	}
	if err := enq.addToKnownJobs(job.Name); err != nil {
		return nil, err
	}

	return job, nil
}

// EnqueueUniqueByKey enqueues a job unless a job is already enqueued with the same name and key, updating arguments.
// The already-enqueued job can be in the normal work queue or in the scheduled job queue.
// Once a worker begins processing a job, another job with the same name and key can be enqueued again.
// Any failed jobs in the retry queue or dead queue don't count against the uniqueness -- so if a job fails and is retried, two unique jobs with the same name and arguments can be enqueued at once.
// In order to add robustness to the system, jobs are only unique for 24 hours after they're enqueued. This is mostly relevant for scheduled jobs.
// EnqueueUniqueByKey returns the job if it was enqueued and nil if it wasn't
func (enq *Enqueuer) EnqueueUniqueByKey(jobName string, args map[string]interface{}, keyMap map[string]interface{}) (*Job, error) {
	var useDefaultKeys bool
	if keyMap == nil {
		useDefaultKeys = true
		keyMap = args
	}

	uniqueKey, err := enq.client.keys.UniqueJobKey(jobName, keyMap)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: time.Now().Unix(),
		Args:       args,
		Unique:     true,
		UniqueKey:  uniqueKey,
	}

	enqueued, err := enq.client.AddUniqueJob(job, useDefaultKeys)
	if err != nil {
		return nil, err
	}
	if !enqueued {
		return nil, ErrDupEnqueued
	}

	if err := enq.addToKnownJobs(jobName); err != nil {
		return nil, err
	}

	return job, nil
}

// EnqueueUniqueInByKey enqueues a job in the scheduled job queue that is unique on specified key for execution in secondsFromNow seconds.
// See EnqueueUnique for the semantics of unique jobs.
// Subsequent calls with same key will update arguments
func (enq *Enqueuer) EnqueueUniqueInByKey(jobName string, secondsFromNow int64, args map[string]interface{}, keyMap map[string]interface{}) (*ScheduledJob, error) {
	var useDefaultKeys bool
	if keyMap == nil {
		useDefaultKeys = true
		keyMap = args
	}

	uniqueKey, err := enq.client.keys.UniqueJobKey(jobName, keyMap)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	job := &ScheduledJob{
		Job: &Job{
			Name:       jobName,
			ID:         makeIdentifier(),
			EnqueuedAt: time.Now().Unix(),
			Args:       args,
			Unique:     true,
			UniqueKey:  uniqueKey,
		},
		RunAt: time.Now().Unix() + secondsFromNow,
	}
	enqueued, err := enq.client.AddUniqueScheduledJob(job, useDefaultKeys)
	if err != nil {
		return nil, err
	}
	if !enqueued {
		return nil, ErrDupEnqueued
	}

	if err := enq.addToKnownJobs(jobName); err != nil {
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

func makeIdentifier() string {
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}
