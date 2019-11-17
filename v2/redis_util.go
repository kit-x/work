package work

import (
	"fmt"
	"strconv"
)

// String is a helper that converts a command reply to a string.
// String converts the reply to a string as follows:
//
//  Reply type      Result
//  bulk string     string(reply), nil
//  simple string   reply, nil
//  other           "",  error
func String(reply interface{}) (string, error) {
	switch reply := reply.(type) {
	case []byte:
		return string(reply), nil
	case string:
		return reply, nil
	}
	return "", fmt.Errorf("redigo: unexpected type for String, got type %T", reply)
}

// Int64 is a helper that converts a command reply to 64 bit integer.
// Int64 converts the reply to an int64 as follows:
//
//  Reply type    Result
//  integer       reply, nil
//  bulk string   parsed reply, nil
//  other         0, error
func Int64(reply interface{}) (int64, error) {
	switch reply := reply.(type) {
	case int64:
		return reply, nil
	case []byte:
		n, err := strconv.ParseInt(string(reply), 10, 64)
		return n, err
	}
	return 0, fmt.Errorf("redigo: unexpected type for Int64, got type %T", reply)
}

func defaultNum(defaultNum int, count ...int) int {
	if len(count) != 0 {
		return count[0]
	}

	return defaultNum
}
