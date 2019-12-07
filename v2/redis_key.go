package work

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

func newKeys(namespace string) *Keys {
	l := len(namespace)
	if (l > 0) && (namespace[l-1] != ':') {
		namespace = namespace + ":"
	}

	return &Keys{
		namespace:           namespace,
		workerPools:         namespace + "worker_pools",
		workerPrefix:        namespace + "worker",
		knownJobs:           namespace + "known_jobs",
		jobsPrefix:          namespace + "jobs:",
		retry:               namespace + "retry",
		dead:                namespace + "dead",
		scheduled:           namespace + "scheduled",
		lastPeriodicEnqueue: namespace + "last_periodic_enqueue",
	}
}

type Keys struct {
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

func (ks Keys) NameSpace() string {
	return ks.namespace
}

func (ks Keys) WorkerPoolsKey() string {
	return ks.workerPools
}

func (ks Keys) RetryKey() string {
	return ks.retry
}

func (ks Keys) DeadKey() string {
	return ks.dead
}

func (ks Keys) ScheduledKey() string {
	return ks.scheduled
}

func (ks Keys) KnownJobsKey() string {
	return ks.knownJobs
}

func (ks Keys) LastPeriodicEnqueueKey(jobName string) string {
	return ks.lastPeriodicEnqueue
}

func (ks Keys) HeartbeatKey(poolID string) string {
	return fmt.Sprintf("%s:%s", ks.workerPools, poolID)
}

func (ks Keys) WorkerObservationKey(workerID string) string {
	return fmt.Sprintf("%s:%s", ks.workerPrefix, workerID)
}

func (ks Keys) JobsInProgressKey(poolID, jobName string) string {
	return fmt.Sprintf("%s:%s:inprogress", ks.JobsKey(jobName), poolID)
}

func (ks Keys) JobsKey(jobName string) string {
	return fmt.Sprintf("%s%s", ks.jobsPrefix, jobName)
}

func (ks Keys) JobsPausedKey(jobName string) string {
	return fmt.Sprintf("%s:paused", ks.JobsKey(jobName))
}

func (ks Keys) JobsLockKey(jobName string) string {
	return fmt.Sprintf("%s:lock", ks.JobsKey(jobName))
}

func (ks Keys) JobsLockInfoKey(jobName string) string {
	return fmt.Sprintf("%s:lock_info", ks.JobsKey(jobName))
}

func (ks Keys) JobsConcurrencyKey(jobName string) string {
	return fmt.Sprintf("%s:max_concurrency", ks.JobsKey(jobName))
}

func (ks Keys) UniqueJobKey(jobName string, args map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	buf.WriteString(ks.namespace)
	buf.WriteString("unique:")
	buf.WriteString(jobName)
	buf.WriteRune(':')

	if args != nil {
		err := json.NewEncoder(&buf).Encode(args)
		if err != nil {
			return "", errors.WithStack(err)
		}
	}

	return buf.String(), nil
}
