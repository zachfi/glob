package chunk

import "fmt"

var (
	ErrChecksumNotMatched   = fmt.Errorf("checksum does not match")
	ErrRelativePathRequired = fmt.Errorf("relative path required")
	ErrAbsolutePathRequired = fmt.Errorf("absolute path required")
)
