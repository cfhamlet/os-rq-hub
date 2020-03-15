package hub

import "fmt"

// ExistError TODO
type ExistError string

func (e ExistError) Error() string {
	return fmt.Sprintf("already exist %s", string(e))
}

// NotExistError TODO
type NotExistError string

func (e NotExistError) Error() string {
	return fmt.Sprintf("not exist %s", string(e))
}

// UnavailableError TODO
type UnavailableError string

func (e UnavailableError) Error() string {
	return fmt.Sprintf("unavailable %s", string(e))
}
