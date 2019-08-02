package fulltext

import (
	"io"
	"io/ioutil"
	"os"
	"os/exec"

	"perkeep.org/pkg/blob"
)

func indexPdf(ref blob.Ref, r io.Reader) (val interface{}, err error) {
	// Use mutool (from muPDF) to convert the pdf to plain text,
	// then hand it off to the text indexer.

	input, err := ioutil.TempFile("", "*.pdf")
	if err != nil {
		return
	}
	inputName := input.Name()
	defer os.Remove(inputName)

	_, err = io.Copy(input, r)
	input.Close()
	if err != nil {
		return
	}

	output, err := ioutil.TempFile("", "*.txt")
	if err != nil {
		return
	}
	outputName := output.Name()
	output.Close()
	defer os.Remove(outputName)

	cmd := exec.Command(
		"mutool", "convert",
		"-F", "text",
		"-o", outputName,
		inputName,
	)
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return
	}
	f, err := os.Open(outputName)
	if err != nil {
		return
	}
	defer f.Close()
	return indexText(ref, f)
}
