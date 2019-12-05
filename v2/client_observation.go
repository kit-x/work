package work

import (
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// WorkerObservations returns all of the WorkerObservation's it finds for all worker pools' workers.
func (client *Client) WorkerObservations() ([]*WorkerObservation, error) {
	workerIDs, err := client.getWorkerIDs()
	if err != nil {
		return nil, err
	}

	cmds := make([]*redis.StringStringMapCmd, 0, len(workerIDs))
	fetchOb := func(pipe redis.Pipeliner) error {
		for i := range workerIDs {
			cmd := pipe.HGetAll(client.keys.WorkerObservationKey(workerIDs[i]))
			cmds = append(cmds, cmd)
		}
		return nil
	}
	if _, err = client.conn.Pipelined(fetchOb); err != nil {
		return nil, errors.WithStack(err)
	}

	obs := make([]*WorkerObservation, 0, len(cmds))
	for _, cmd := range cmds {
		result, err := cmd.Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		obs = append(obs, newWorkerObservation(result))
	}

	return obs, nil
}

func (client *Client) getWorkerIDs() ([]string, error) {
	beats, err := client.WorkerPoolHeartbeats()
	if err != nil {
		return nil, err
	}

	// TODO: workers count should be set ?
	workerIDs := make([]string, 0, len(beats)*2)
	for _, beat := range beats {
		workerIDs = append(workerIDs, beat.WorkerIDs...)
	}

	return workerIDs, nil
}
