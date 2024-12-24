package s3

import (
	"io/fs"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestIsNotExists(t *testing.T) {
	nsk := s3types.NoSuchKey{Message: aws.String("no shuch key mate")}
	if isNotExists(&nsk) != true {
		t.Errorf("isNotExists is not true")
	}
	perr := fs.ErrInvalid
	if isNotExists(perr) == true {
		t.Errorf("isNotExists should not true")
	}
}
