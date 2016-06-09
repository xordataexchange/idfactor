package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"xor/lib/idfactor"
	"xor/lib/idfactor/atrisk"
	"xor/lib/idfactor/compromised"
)

var usage = func() {
	str := `usage: idfactor [-c] [-d delimiter] [-m file] [-o directory] [file]

Split each identity record into pieces and output them in shuffled order.

The input file is a delimited text file with column headers where each row
contains a full identity record consisting of a unique record identifier
followed by name, date of birth, ssn, address, phone number, and email address
fields. If a file is not supplied then it is read from the standard input.
Additionally, if -c is specified then compromised entity input format is
assumed. This formats adds a breach identifier after the record identifier.

Output files are written to the current working directory unless an output
directory is specified with -o.

Optionally specify -m to write a map file that can be used to reconstruct the
full identity record from the identity elements.

`
	fmt.Fprint(os.Stderr, str)
	flag.PrintDefaults()
}

func main() {
	var (
		delim           string
		mapfile         string
		dir             string
		fieldsPerRecord int
		comp            bool
		factorID        func([][]string, string) ([][]string, error)
	)

	flag.StringVar(&delim, "d", "|", "field `delimiter` for the input file")
	flag.StringVar(&mapfile, "m", "", "write an identity map to the named `file`")
	flag.StringVar(&dir, "o", "", "write the identity elements to the named `directory`")
	flag.BoolVar(&comp, "c", false, "use compromised entity input format")
	flag.Usage = usage
	flag.Parse()

	// check at-risk or compromised mode
	if comp {
		fieldsPerRecord = compromised.RecordLength
		factorID = compromised.IDFactor
	} else {
		fieldsPerRecord = atrisk.RecordLength
		factorID = atrisk.IDFactor
	}

	// check for single char delimiter
	if len(delim) != 1 {
		log.Fatal("delimiter must be exactly one character")
	}

	var (
		in  io.ReadCloser
		err error
	)
	if flag.Arg(0) == "" {
		// read from stdin if no input file supplied
		in = os.Stdin
	} else {
		// otherwise open the file for reading
		in, err = os.Open(flag.Arg(0))
		if err != nil {
			log.Fatalf("error opening input file: %s", err)
		}
	}

	reader := csv.NewReader(in)
	reader.Comma = rune(delim[0])
	reader.FieldsPerRecord = fieldsPerRecord

	// read and discard header
	if _, err = reader.Read(); err != nil {
		log.Fatalf("error reading file : %s", err)
	}

	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("error reading file: %s", err)
	}
	if err := in.Close(); err != nil {
		log.Fatalf("error closing file: %s", err)
	}

	ids, err := factorID(records, dir)
	if err != nil {
		log.Fatalf("error factoring ids: %s", err)
	}

	if mapfile != "" {
		filename := filepath.Join(dir, mapfile)
		idfactor.WriteMapToFile(ids, filename)
	}
}
