package work

import "github.com/pkg/errors"

// ErrNotDeleted is returned by functions that delete jobs to indicate that although the redis commands were successful,
// no object was actually deleted by those commmands.
var ErrNotDeleted = errors.New("nothing deleted")

// ErrNotRetried is returned by functions that retry jobs to indicate that although the redis commands were successful,
// no object was actually retried by those commmands.
var ErrNotRetried = errors.New("nothing retried")

// ErrDupEnqueued is returned by functions that enqueue duplicate job that already enqueued with the same name and key
var ErrDupEnqueued = errors.New("enqueue duplicate unique job")

// ErrEmptyUniqueKey is returned by functions that enqueue unique job that with empty unique key
var ErrEmptyUniqueKey = errors.New("enqueue duplicate unique job")
