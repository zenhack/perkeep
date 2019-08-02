package fulltext

import (
	"io"
	"io/ioutil"

	"perkeep.org/pkg/blob"
)

func indexText(ref blob.Ref, r io.Reader) (interface{}, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return string(data), nil
}
