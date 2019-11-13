package work

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func newKeys(namespace string) *keys {
	l := len(namespace)
	if (l > 0) && (namespace[l-1] != ':') {
		namespace = namespace + ":"
	}

	return &keys{
		namespace:           namespace,
		workerPools:         namespace + "worker_pools",
		workerPrefix:        namespace + "worker",
		knownJobs:           namespace + "known_jobs",
		jobsPrefix:          namespace + "jobs",
		retry:               namespace + "retry",
		dead:                namespace + "retry",
		scheduled:           namespace + "scheduled",
		lastPeriodicEnqueue: namespace + "last_periodic_enqueue",
	}
}

type keys struct {
	namespace           string
	workerPools         string
	workerPrefix        string
	knownJobs           string
	jobsPrefix          string
	retry               string
	dead                string
	scheduled           string
	lastPeriodicEnqueue string
}

func (ks keys) NameSpace() string {
	return ks.namespace
}

func (ks keys) WorkerPoolsKey() string {
	return ks.workerPools
}

func (ks keys) RetryKey() string {
	return ks.retry
}

func (ks keys) DeadKey() string {
	return ks.dead
}

func (ks keys) ScheduledKey() string {
	return ks.scheduled
}

func (ks keys) KnownJobsKey() string {
	return ks.knownJobs
}

func (ks keys) LastPeriodicEnqueueKey(jobName string) string {
	return ks.lastPeriodicEnqueue
}

func (ks keys) HeartbeatKey(poolID string) string {
	return fmt.Sprintf("%s:%s", ks.workerPools, poolID)
}

func (ks keys) WorkerObservationKey(workerID string) string {
	return fmt.Sprintf("%s:%s", ks.workerPrefix, workerID)
}

func (ks keys) JobsInProgressKey(poolID, jobName string) string {
	return fmt.Sprintf("%s:%s:inprogress", ks.JobsKey(jobName), poolID)
}

func (ks keys) JobsKey(jobName string) string {
	return fmt.Sprintf("%s:%s", ks.jobsPrefix, jobName)
}

func (ks keys) JobsPausedKey(jobName string) string {
	return fmt.Sprintf("%s:paused", ks.JobsKey(jobName))
}

func (ks keys) JobsLockKey(jobName string) string {
	return fmt.Sprintf("%s:lock", ks.JobsKey(jobName))
}

func (ks keys) JobsLockInfoKey(jobName string) string {
	return fmt.Sprintf("%s:lock_info", ks.JobsKey(jobName))
}

func (ks keys) JobsConcurrencyKey(jobName string) string {
	return fmt.Sprintf("%s:max_concurrency", ks.JobsKey(jobName))
}

func (ks keys) UniqueJobKey(jobName string, args map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	buf.WriteString(ks.namespace)
	buf.WriteString("unique:")
	buf.WriteString(jobName)
	buf.WriteRune(':')

	if args != nil {
		err := json.NewEncoder(&buf).Encode(args)
		if err != nil {
			return "", err
		}
	}

	return buf.String(), nil
}
