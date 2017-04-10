package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"xor/lib/idfactor"
	"xor/lib/idfactor/atrisk"
	"xor/lib/idfactor/compromised"
)

// Output file names
const (
	NameDobFile     = "name_dob_elements.psv"
	SsnFile         = "ssn_elements.psv"
	AddressFile     = "address_elements.psv"
	PhoneFile       = "phone_elements.psv"
	EmailFile       = "email_elements.psv"
	NameAddressFile = "name_address_elements.psv"
	NamePhoneFile   = "name_phone_elements.psv"
	UserNameFile    = "username_elements.psv"
)

//------------------------------------------------------------------------------
// ID factoring for at-risk entities
//------------------------------------------------------------------------------

func AtRiskNameDob(recs [][]string) map[string]string {
	return atrisk.WriteNameDobFile(recs, NameDobFile)
}

func AtRiskSsn(recs [][]string) map[string]string {
	return atrisk.WriteSsnFile(recs, SsnFile)
}

func AtRiskAddress(recs [][]string) map[string]string {
	return atrisk.WriteAddressFile(recs, AddressFile)
}

func AtRiskPhone(recs [][]string) map[string]string {
	return atrisk.WritePhoneFile(recs, PhoneFile)
}

func AtRiskEmail(recs [][]string) map[string]string {
	return atrisk.WriteEmailFile(recs, EmailFile)
}

func AtRiskNameAddress(recs [][]string) map[string]string {
	return atrisk.WriteNameAddressFile(recs, NameAddressFile)
}

func AtRiskNamePhone(recs [][]string) map[string]string {
	return atrisk.WriteNamePhoneFile(recs, NamePhoneFile)
}

func AtRiskUserName(recs [][]string) map[string]string {
	return atrisk.WriteUserNameFile(recs, UserNameFile)
}

func AtRiskIDFactoring(recs [][]string) (idmap [][]string, err error) {
	return idfactor.IDFactor(recs, AtRiskNameDob, AtRiskSsn, AtRiskAddress, AtRiskPhone, AtRiskEmail, AtRiskNameAddress, AtRiskNamePhone, AtRiskUserName)
}

//------------------------------------------------------------------------------
// ID factoring for compromised entities
//------------------------------------------------------------------------------

func CompromisedNameDob(recs [][]string) map[string]string {
	return compromised.WriteNameDobFile(recs, NameDobFile)
}

func CompromisedSsn(recs [][]string) map[string]string {
	return compromised.WriteSsnFile(recs, SsnFile)
}

func CompromisedAddress(recs [][]string) map[string]string {
	return compromised.WriteAddressFile(recs, AddressFile)
}

func CompromisedPhone(recs [][]string) map[string]string {
	return compromised.WritePhoneFile(recs, PhoneFile)
}

func CompromisedEmail(recs [][]string) map[string]string {
	return compromised.WriteEmailFile(recs, EmailFile)
}

func CompromisedNameAddress(recs [][]string) map[string]string {
	return compromised.WriteNameAddressFile(recs, NameAddressFile)
}

func CompromisedNamePhone(recs [][]string) map[string]string {
	return compromised.WriteNamePhoneFile(recs, NamePhoneFile)
}

func CompromisedUserName(recs [][]string) map[string]string {
	return compromised.WriteUserNameFile(recs, UserNameFile)
}

func CompromisedIDFactoring(recs [][]string) (idmap [][]string, err error) {
	return idfactor.IDFactor(recs, CompromisedNameDob, CompromisedSsn, CompromisedAddress, CompromisedPhone, CompromisedEmail, CompromisedNameAddress, CompromisedNamePhone, CompromisedUserName)
}

//------------------------------------------------------------------------------
// Command line tool
//------------------------------------------------------------------------------

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
		isCompromised   bool
		factor          func([][]string) ([][]string, error)
	)

	flag.StringVar(&delim, "d", "|", "field `delimiter` for the input file")
	flag.StringVar(&mapfile, "m", "", "write an identity map to the named `file`")
	flag.StringVar(&dir, "o", "", "write the identity elements to the named `directory`")
	flag.BoolVar(&isCompromised, "c", false, "use compromised entity input format")
	flag.Usage = usage
	flag.Parse()

	// check at-risk or compromised mode
	if isCompromised {
		fieldsPerRecord = compromised.RecordLength
		factor = CompromisedIDFactoring
	} else {
		fieldsPerRecord = atrisk.RecordLength
		factor = AtRiskIDFactoring
	}

	// check for single char delimiter
	if len(delim) != 1 {
		log.Fatal("delimiter must be exactly one character")
	}

	// read input from stdin or file
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

	// read all records
	reader := csv.NewReader(in)
	reader.Comma = rune(delim[0])
	reader.FieldsPerRecord = fieldsPerRecord
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

	// change to output directory and write output
	if dir != "" {
		if err := os.Chdir(dir); err != nil {
			log.Fatalf(`error setting working directory to "%s":`, err)
		}
	}
	ids, err := factor(records)
	if err != nil {
		log.Fatalf("error factoring ids: %s", err)
	}
	if mapfile != "" {
		idfactor.WriteMapToFile(ids, mapfile)
	}
}
