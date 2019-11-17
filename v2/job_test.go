package work

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_ScheduledJobs(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	total := int(client.options.ScheduledJobPageSize + 1)
	pageSize := int(client.options.ScheduledJobPageSize)
	gotJobs := make([]*ScheduledJob, 0, total)
	jobs := client.mockScheduledJobs(total)
	require.Equal(t, int(total), len(jobs))

	// page 1 with 20 jobs
	page1, gotTotal, err := client.ScheduledJobs(1)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, pageSize, len(page1))
	gotJobs = append(gotJobs, page1...)

	// page 2 with 1 job
	page2, gotTotal, err := client.ScheduledJobs(2)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, 1, len(page2))
	gotJobs = append(gotJobs, page2...)

	// page 3 with 0 job
	page3, gotTotal, err := client.ScheduledJobs(3)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, 0, len(page3))

	require.Equal(t, total, len(gotJobs))
}

func TestClient_RetryJobs(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	total := int(client.options.RetryJobPageSize + 1)
	pageSize := int(client.options.RetryJobPageSize)
	gotJobs := make([]*RetryJob, 0, total)
	jobs := client.mockRetryJobs(total)
	require.Equal(t, int(total), len(jobs))

	// page 1 with 20 jobs
	page1, gotTotal, err := client.RetryJobs(1)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, pageSize, len(page1))
	gotJobs = append(gotJobs, page1...)

	// page 2 with 1 job
	page2, gotTotal, err := client.RetryJobs(2)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, 1, len(page2))
	gotJobs = append(gotJobs, page2...)

	// page 3 with 0 job
	page3, gotTotal, err := client.RetryJobs(3)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, 0, len(page3))

	require.Equal(t, total, len(gotJobs))
}

func TestClient_DeadJobs(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	total := int(client.options.DeadJobPageSize + 1)
	pageSize := int(client.options.DeadJobPageSize)
	gotJobs := make([]*DeadJob, 0, total)
	jobs := client.mockDeadJobs(total)
	require.Equal(t, int(total), len(jobs))

	// page 1 with 20 jobs
	page1, gotTotal, err := client.DeadJobs(1)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, pageSize, len(page1))
	gotJobs = append(gotJobs, page1...)

	// page 2 with 1 job
	page2, gotTotal, err := client.DeadJobs(2)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, 1, len(page2))
	gotJobs = append(gotJobs, page2...)

	// page 3 with 0 job
	page3, gotTotal, err := client.DeadJobs(3)
	require.NoError(t, err)
	require.Equal(t, int64(total), gotTotal)
	require.Equal(t, 0, len(page3))

	require.Equal(t, total, len(gotJobs))
}

func TestClient_DeleteDeadJob(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	jobs := client.mockDeadJobs()
	total, _ := client.conn.ZCard(client.keys.dead).Result()
	require.Equal(t, len(jobs), int(total))

	// delete 1 job
	err := client.DeleteDeadJob(jobs[0].DiedAt, jobs[0].ID)
	require.NoError(t, err)
	total, _ = client.conn.ZCard(client.keys.dead).Result()
	require.Equal(t, len(jobs)-1, int(total))

	// delete failed when job id not found
	err = client.DeleteDeadJob(jobs[0].DiedAt, "not found")
	assert.EqualError(t, ErrNotDeleted, errors.Cause(err).Error())

	// delete failed when job die_at invalid
	err = client.DeleteDeadJob(0, jobs[0].ID)
	assert.EqualError(t, ErrNotDeleted, errors.Cause(err).Error())
}

func TestClient_DeleteRetryJob(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	jobs := client.mockRetryJobs()
	total, _ := client.conn.ZCard(client.keys.retry).Result()
	require.Equal(t, len(jobs), int(total))

	// delete 1 job
	err := client.DeleteRetryJob(jobs[0].RetryAt, jobs[0].ID)
	require.NoError(t, err)
	total, _ = client.conn.ZCard(client.keys.retry).Result()
	require.Equal(t, len(jobs)-1, int(total))

	// delete failed when job id not found
	err = client.DeleteDeadJob(jobs[0].RetryAt, "not found")
	assert.EqualError(t, ErrNotDeleted, errors.Cause(err).Error())

	// delete failed when job die_at invalid
	err = client.DeleteDeadJob(0, jobs[0].ID)
	assert.EqualError(t, ErrNotDeleted, errors.Cause(err).Error())

	// nothing in dead set
	client.cleanup()
	err = client.DeleteDeadJob(0, jobs[0].ID)
	assert.EqualError(t, ErrNotDeleted, errors.Cause(err).Error())
}

func TestClient_DeleteScheduledJob(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	// mock unique job
	jobs := client.mockUniqueScheduledJobs(1)
	total, _ := client.conn.ZCard(client.keys.scheduled).Result()
	require.Equal(t, 1, int(total))

	// delete 1 unique job
	err := client.DeleteScheduledJob(jobs[0].RunAt, jobs[0].ID)
	require.NoError(t, err)
	total, _ = client.conn.ZCard(client.keys.scheduled).Result()
	require.Equal(t, 0, int(total))

	// mock 1 job
	jobs = client.mockScheduledJobs(1)
	total, _ = client.conn.ZCard(client.keys.scheduled).Result()
	require.Equal(t, 1, int(total))

	// delete 1 job
	err = client.DeleteScheduledJob(jobs[0].RunAt, jobs[0].ID)
	require.NoError(t, err)
	total, _ = client.conn.ZCard(client.keys.scheduled).Result()
	require.Equal(t, 0, int(total))

	// delete failed when job id not found
	err = client.DeleteScheduledJob(jobs[0].RunAt, "not found")
	assert.EqualError(t, ErrNotDeleted, errors.Cause(err).Error())

	// delete failed when job die_at invalid
	err = client.DeleteScheduledJob(0, jobs[0].ID)
	assert.EqualError(t, ErrNotDeleted, errors.Cause(err).Error())
}

func TestClient_RetryDeadJob(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	// remove 1 job from dead job & push into job's List
	jobs := client.mockDeadJobs()
	total, _ := client.conn.ZCard(client.keys.dead).Result()
	require.Equal(t, len(jobs), int(total))
	jobSize, _ := client.conn.LLen(jobs[0].Name).Result()
	require.Equal(t, int64(0), jobSize)

	err := client.RetryDeadJob(jobs[0].DiedAt, jobs[0].ID)
	require.NoError(t, err)

	total, _ = client.conn.ZCard(client.keys.dead).Result()
	require.Equal(t, len(jobs)-1, int(total))
	jobSize, _ = client.conn.LLen(client.keys.JobsKey(jobs[0].Name)).Result()
	require.Equal(t, int64(1), jobSize)

	// remove invalid job id
	err = client.RetryDeadJob(jobs[0].DiedAt, "not found")
	assert.EqualError(t, ErrNotRetried, errors.Cause(err).Error())

	// remove invalid job die_at
	err = client.RetryDeadJob(0, jobs[0].ID)
	assert.EqualError(t, ErrNotRetried, errors.Cause(err).Error())

	// nothing in dead set
	client.cleanup()
	err = client.RetryDeadJob(jobs[0].DiedAt, jobs[0].ID)
	assert.EqualError(t, ErrNotRetried, errors.Cause(err).Error())
}

func TestClient_DeleteAllDeadJobs(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	jobs := client.mockDeadJobs()
	total, _ := client.conn.ZCard(client.keys.dead).Result()
	require.Equal(t, len(jobs), int(total))

	err := client.DeleteAllDeadJobs()
	require.NoError(t, err)

	total, _ = client.conn.ZCard(client.keys.dead).Result()
	require.Equal(t, 0, int(total))
}

func TestClient_RetryAllDeadJobs(t *testing.T) {
	client := newTestClient()
	defer client.cleanup()

	jobs := client.mockDeadJobs()
	total, _ := client.conn.ZCard(client.keys.dead).Result()
	require.Equal(t, len(jobs), int(total))
	for _, job := range jobs {
		jobSize, _ := client.conn.LLen(job.Name).Result()
		require.Equal(t, int64(0), jobSize)
	}

	err := client.RetryAllDeadJobs()
	require.NoError(t, err)

	for _, job := range jobs {
		jobSize, _ := client.conn.LLen(client.keys.JobsKey(job.Name)).Result()
		require.Equal(t, int64(1), jobSize)
	}
}
