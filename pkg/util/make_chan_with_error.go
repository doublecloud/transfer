package util

// MakeChanWithError constructs a buffered channel with capacity 1, writes the given error into it and returns the channel.
func MakeChanWithError(err error) chan error {
	result := make(chan error, 1)
	result <- err
	return result
}
