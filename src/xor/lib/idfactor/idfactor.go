package idfactor

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"runtime"

	"xor/lib/shuffle"
	"xor/lib/uuid"
)

const (
	// RecordIDField is the field position of the record ID
	RecordIDField = iota
	// FirstNameField is the field position of the record first name
	FirstNameField
	// LastNameField is the field position of the record last name
	LastNameField
	// MiddleInitialField is the field position of the record middle initial
	MiddleInitialField
	// SuffixField is the field position of the record suffix
	SuffixField
	// DobField is the field position of the record date of birth
	DobField
	// SsnField is the field position of the record ssn
	SsnField
	// AddressLine1Field is the field position of the record address line 1
	AddressLine1Field
	// AddressLine2Field is the field position of the record address line 2
	AddressLine2Field
	// CityField is the field position of the record city
	CityField
	// StateField is the field psoition of the record state
	StateField
	// ZipField is the field position of the record zip code
	ZipField
	// PhoneField is the field position of the record phone number
	PhoneField
	// EmailField is the field position of the record email address
	EmailField
	// RecordLength is the number of fields in a record
	RecordLength
)

var (
	nameHeader    = []string{"name_id", "first_name", "last_name", "middle_initial", "suffix", "dob"}
	ssnHeader     = []string{"ssn_id", "ssn"}
	addressHeader = []string{"address_id", "address_line_1", "address_line_2", "city", "state", "zip", "zip4"}
	phoneHeader   = []string{"phone_id", "phone"}
	emailHeader   = []string{"email_id", "email"}
	mapHeader     = []string{"record_id", "name_id", "ssn_id", "address_id", "phone_id", "email_id"}
)

//------------------------------------------------------------------------------
// Identity element getters. These functions extract an identity element from
// a full identity record and insert the given ID in the returned element.
//------------------------------------------------------------------------------

// ElementGetter is the function signature of functions that extract an
// identity element from a full identity record.
type ElementGetter func(rec []string, id string) []string

// ToNameDob extracts a name and dob identity element from the given full
// identity record. The extracted element is assigned the given id.
func ToNameDob(rec []string, id string) []string {
	checkLength(rec)
	if none(rec[FirstNameField], rec[LastNameField], rec[MiddleInitialField], rec[SuffixField], rec[DobField]) {
		return nil
	}
	return []string{id, rec[FirstNameField], rec[LastNameField], rec[MiddleInitialField], rec[SuffixField], rec[DobField]}
}

// ToSsn extracts an ssn identity element from the given full identity record.
// The extracted element is assigned the given id.
func ToSsn(rec []string, id string) []string {
	checkLength(rec)
	if none(rec[SsnField]) {
		return nil
	}
	return []string{id, rec[SsnField]}
}

// ToAddress extracts an address identity element from the given full identity
// record. The extracted element is assigned the given id.
func ToAddress(rec []string, id string) []string {
	checkLength(rec)
	if none(rec[AddressLine1Field], rec[AddressLine2Field], rec[CityField], rec[StateField], rec[ZipField]) {
		return nil
	}
	return []string{id, rec[AddressLine1Field], rec[AddressLine2Field], rec[CityField], rec[StateField], rec[ZipField], ""}
}

// ToPhone extracts a phone identity element from the given full identity
// record. The extracted element is assigned the given id.
func ToPhone(rec []string, id string) []string {
	checkLength(rec)
	if none(rec[PhoneField]) {
		return nil
	}
	return []string{id, rec[PhoneField]}
}

// ToEmail extracts an email identity element from the given full identity
// record. The extracted element is assigned the given id.
func ToEmail(rec []string, id string) []string {
	checkLength(rec)
	if none(rec[EmailField]) {
		return nil
	}
	return []string{id, rec[EmailField]}
}

//------------------------------------------------------------------------------
// These functions extract identity elements from a list of full identity
// records and write them to the named file in shuffled order. They return a
// map from record ids to element ids.
//------------------------------------------------------------------------------

// WriteNameDobFile extracts name and dob identity elements from a list of full
// identity elements and writes them to the named file in shuffled order. It
// returns a map from record ids to element ids.
func WriteNameDobFile(recs [][]string, name string) map[string]string {
	return writeToFile(recs, name, nameHeader, ToNameDob)
}

// WriteSsnFile extracts ssn identity elements from a list of full identity
// elements and writes them to the named file in shuffled order. It returns a
// map from record ids to element ids.
func WriteSsnFile(recs [][]string, name string) map[string]string {
	return writeToFile(recs, name, ssnHeader, ToSsn)
}

// WriteAddressFile extracts address identity elements from a list of full
// identity elements and writes them to the named file in shuffled order. It
// returns a map from record ids to element ids.
func WriteAddressFile(recs [][]string, name string) map[string]string {
	return writeToFile(recs, name, addressHeader, ToAddress)
}

// WritePhoneFile extracts phone identity elements from a list of full identity
// elements and writes them to the named file in shuffled order. It returns a
// map from record ids to element ids.
func WritePhoneFile(recs [][]string, name string) map[string]string {
	return writeToFile(recs, name, phoneHeader, ToPhone)
}

// WriteEmailFile extracts email identity elements from a list of full identity
// elements and writes them to the named file in shuffled order. It returns a
// map from record ids to element ids.
func WriteEmailFile(recs [][]string, name string) map[string]string {
	return writeToFile(recs, name, emailHeader, ToEmail)
}

//------------------------------------------------------------------------------
// These functions extract identity elements from a list of full identity
// records and write them to the named file in shuffled order. They return a
// map from record ids to element ids.
//------------------------------------------------------------------------------

// WriteNameDob extracts name and dob identity elements from a list of full
// identity elements and writes them to the given io.Writer in shuffled order.
// It returns a map from record ids to element ids.
func WriteNameDob(recs [][]string, writer io.Writer) map[string]string {
	return writeToWriter(recs, writer, nameHeader, ToNameDob)
}

// WriteSsn extracts ssn identity elements from a list of full identity
// elements and writes them to the given io.Writer in shuffled order. It returns
// a map from record ids to element ids.
func WriteSsn(recs [][]string, writer io.Writer) map[string]string {
	return writeToWriter(recs, writer, ssnHeader, ToSsn)
}

// WriteAddress extracts address identity elements from a list of full identity
// elements and writes them to the given io.Writer in shuffled order. It returns
// a map from record ids to element ids.
func WriteAddress(recs [][]string, writer io.Writer) map[string]string {
	return writeToWriter(recs, writer, addressHeader, ToAddress)
}

// WritePhone extracts phone identity elements from a list of full identity
// elements and writes them to the given io.Writer in shuffled order. It returns
// a map from record ids to element ids.
func WritePhone(recs [][]string, writer io.Writer) map[string]string {
	return writeToWriter(recs, writer, phoneHeader, ToPhone)
}

// WriteEmail extracts email identity elements from a list of full identity
// elements and writes them to the given io.Writer in shuffled order. It returns
// a map from record ids to element ids.
func WriteEmail(recs [][]string, writer io.Writer) map[string]string {
	return writeToWriter(recs, writer, emailHeader, ToEmail)
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------

// WriteMapToFile writes a pipe-delimited identity element mapping file with the
// given name.
func WriteMapToFile(recs [][]string, name string, nameid, ssnid, addressid, phoneid, emailid map[string]string) {
	file, err := os.Create(name)
	if err != nil {
		log.Fatalf(`idfactor: error creating file "%s": %s`, name, err)
	}
	WriteMapToWriter(recs, file, nameid, ssnid, addressid, phoneid, emailid)
	if err := file.Close(); err != nil {
		log.Fatalf(`idfactor: error closing file "%s": %s`, name, err)
	}
}

// WriteMapToWriter writers a pipe-delimited identity element mapping file to
// the given io.Writer.
func WriteMapToWriter(recs [][]string, w io.Writer, nameid, ssnid, addressid, phoneid, emailid map[string]string) {
	writer := csv.NewWriter(w)
	writer.Comma = '|'
	// line terminator
	if runtime.GOOS == "windows" {
		writer.UseCRLF = true
	}
	// write file header
	if err := writer.Write(mapHeader); err != nil {
		log.Fatalf("idfactor: error writing file: %s", err)
	}
	for _, rec := range recs {
		id := rec[RecordIDField]
		out := []string{id, nameid[id], ssnid[id], addressid[id], phoneid[id], emailid[id]}
		if err := writer.Write(out); err != nil {
			log.Fatalf("idfactor: error writing file: %s", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatalf("idfactor: error flushing output writer: %s", err)
	}
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------

// writeTofiles extracts identity elements from a list of full identity records
// and writes them to the named file. It returns a map from record ids to
// element ids.
func writeToFile(recs [][]string, name string, header []string, get ElementGetter) map[string]string {
	file, err := os.Create(name)
	if err != nil {
		log.Fatalf(`idfactor: error creating file "%s": %s`, name, err)
	}
	result := writeToWriter(recs, file, header, get)
	if err := file.Close(); err != nil {
		log.Fatalf(`idfactor: error closing file "%s": %s`, name, err)
	}
	return result
}

// writeTofiles extracts identity elements from a list of full identity records
// and writes them to the given io.Writer. It returns a map from record ids to
// element ids.
func writeToWriter(recs [][]string, w io.Writer, header []string, get ElementGetter) map[string]string {
	writer := csv.NewWriter(w)
	writer.Comma = '|'
	// line terminator
	if runtime.GOOS == "windows" {
		writer.UseCRLF = true
	}
	// write file header
	if err := writer.Write(header); err != nil {
		log.Fatalf("idfactor: error writing file: %s", err)
	}

	// map record ids to element ids
	idmap := make(map[string]string)
	// write elements in shuffled order
	for _, i := range shuffle.Shuffle(len(recs)) {
		// generate a new uuid for element id
		elemid := uuid.New()
		id := recs[i][RecordIDField]
		// only write non-nil elements
		if elem := get(recs[i], elemid); elem != nil {
			if err := writer.Write(elem); err != nil {
				log.Fatalf(`idfactor: error writing element: %s`, err)
			}
			// update id mapping
			idmap[id] = elemid
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatalf(`idfactor: error flushing writer: %s`, err)
	}
	return idmap
}

//------------------------------------------------------------------------------
// utility functions
//------------------------------------------------------------------------------

// halt execution if the given record has an incorrect number of fields
func checkLength(rec []string) {
	if n := len(rec); n != RecordLength {
		log.Fatalf("idfactor: bad record length (expected %d, got %d)", RecordLength, n)
	}
}

// returns true if and only if a nonempty string is supplied
func any(strs ...string) bool {
	for _, s := range strs {
		if s != "" {
			return true
		}
	}
	return false
}

// returns true if and only if no nonempty strings are supplied
func none(strs ...string) bool {
	return !any(strs...)
}
