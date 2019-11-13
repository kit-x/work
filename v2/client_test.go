package work

import (
	"fmt"
)

// cleanup when in testing. it should only used in test
func (c *Client) cleanup() {
	keys, err := c.conn.Keys(fmt.Sprintf("%s*", c.keys.NameSpace())).Result()
	if err != nil {
		panic(err)
	}

	if len(keys) == 0 {
		return
	}

	if err := c.conn.Del(keys...).Err(); err != nil {
		panic(err)
	}
}

func (c *Client) mockWorkerPoolHeartbeat() *WorkerPoolHeartbeat {
	heartbeat := fakeWorkerPoolHeartbeat()
	if err := c.setWorkerPoolIDs(heartbeat.WorkerPoolID); err != nil {
		panic(err)
	}

	if err := c.setWorkerPoolHeartbeat(heartbeat); err != nil {
		panic(err)
	}

	return heartbeat
}

func (c *Client) setWorkerPoolIDs(ids ...string) error {
	return c.conn.SAdd(c.keys.WorkerPoolsKey(), ids).Err()
}

func (c *Client) setWorkerPoolHeartbeat(heartbeat *WorkerPoolHeartbeat) error {
	return c.conn.HMSet(c.keys.HeartbeatKey(heartbeat.WorkerPoolID), heartbeat.ToRedis()).Err()
}

func (c *Client) setWorkerObservation(ob *WorkerObservation) error {
	return c.conn.HMSet(c.keys.WorkerObservationKey(ob.WorkerID), ob.ToRedis()).Err()
}

func (c *Client) mockWorkerObservation() *WorkerObservation {
	heartbeat := c.mockWorkerPoolHeartbeat()

	ob := fakeWorkerObservation()
	ob.heartbeat = heartbeat
	ob.WorkerID = heartbeat.WorkerIDs[0]
	ob.JobName = heartbeat.JobNames[0]
	if err := c.setWorkerObservation(ob); err != nil {
		panic(err)
	}

	return ob
}
