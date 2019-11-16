package work

import (
	"testing"

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
