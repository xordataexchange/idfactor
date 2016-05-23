package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"lib/idfactor"
)

var usage = func() {
	str := `usage: idfactor [-d delimiter] [-m file] [-o directory] [file]

Split each identity record into pieces and output them in shuffled order.

The input file is a delimited text file with column headers where each row
contains a full identity record consisting of a unique record identifier
followed by name, date of birth, ssn, address, phone number, and email address
fields. If a file is not supplied then it is read from the standard input.

Output files are written to the current working directory unless an output
directory is specified with -o.

Optionally specify -m to write a map file that can be used to reconstruct the
full identity record from the identity elements.

`
	fmt.Fprint(os.Stderr, str)
	flag.PrintDefaults()
}

func main() {
	// command line arguments
	var (
		delim   string
		mapfile string
		dir     string
	)
	flag.StringVar(&delim, "d", "|", "field `delimiter` for the input file")
	flag.StringVar(&mapfile, "m", "", "write an identity map to the named `file`")
	flag.StringVar(&dir, "o", "", "write the identity elements to the named `directory`")
	flag.Usage = usage
	flag.Parse()

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
	reader.FieldsPerRecord = idfactor.RecordLength
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

	// maps from record id to element id
	var (
		nameid    map[string]string
		ssnid     map[string]string
		addressid map[string]string
		phoneid   map[string]string
		emailid   map[string]string
	)

	workers := sync.WaitGroup{}
	workers.Add(5)
	go func() {
		filename := filepath.Join(dir, "name_dob_elements.psv")
		nameid = idfactor.WriteNameDobFile(records, filename)
		workers.Done()
	}()
	go func() {
		filename := filepath.Join(dir, "ssn_elements.psv")
		ssnid = idfactor.WriteSsnFile(records, filename)
		workers.Done()
	}()
	go func() {
		filename := filepath.Join(dir, "address_elements.psv")
		addressid = idfactor.WriteAddressFile(records, filename)
		workers.Done()
	}()
	go func() {
		filename := filepath.Join(dir, "phone_elements.psv")
		phoneid = idfactor.WritePhoneFile(records, filename)
		workers.Done()
	}()
	go func() {
		filename := filepath.Join(dir, "email_elements.psv")
		emailid = idfactor.WriteEmailFile(records, filename)
		workers.Done()
	}()
	workers.Wait()

	if mapfile != "" {
		filename := filepath.Join(dir, mapfile)
		idfactor.WriteMapToFile(records, filename, nameid, ssnid, addressid, phoneid, emailid)
	}
}
