package compromised

import (
	"io"
	"log"
	"path/filepath"

	"xor/lib/idfactor"
)

const (
	// RecordIDField is the field position of the record ID
	RecordIDField = iota
	// BreachIDField is the field position of the breach ID
	BreachIDField
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
	nameHeader        = []string{"breach_id", "name_id", "first_name", "last_name", "middle_initial", "suffix", "dob"}
	ssnHeader         = []string{"breach_id", "ssn_id", "ssn"}
	addressHeader     = []string{"breach_id", "address_id", "address_line_1", "address_line_2", "city", "state", "zip", "zip4"}
	phoneHeader       = []string{"breach_id", "phone_id", "phone"}
	emailHeader       = []string{"breach_id", "email_id", "email"}
	nameAddressHeader = []string{"breach_id", "name_address_id", "first_name", "last_name", "middle_initial", "suffix", "address_line_1", "address_line_2", "city", "state", "zip", "zip4"}
	namePhoneHeader   = []string{"breach_id", "name_phone_id", "first_name", "last_name", "middle_initial", "suffix", "phone"}
)

//------------------------------------------------------------------------------
// Identity element getters. These functions extract an identity element from
// a full identity record and insert the given ID in the returned element.
//------------------------------------------------------------------------------

// ToNameDob extracts a name and dob identity element from the given full
// identity record. The extracted element is assigned the given id.
func ToNameDob(rec []string, id string) []string {
	checkLength(rec)
	fields := []string{
		rec[FirstNameField],
		rec[LastNameField],
		rec[MiddleInitialField],
		rec[SuffixField],
		rec[DobField],
	}
	if idfactor.None(fields...) {
		return nil
	}
	return append([]string{rec[BreachIDField], id}, fields...)
}

// ToSsn extracts an ssn identity element from the given full identity record.
// The extracted element is assigned the given id.
func ToSsn(rec []string, id string) []string {
	checkLength(rec)
	if idfactor.None(rec[SsnField]) {
		return nil
	}
	return []string{rec[BreachIDField], id, rec[SsnField]}
}

// ToAddress extracts an address identity element from the given full identity
// record. The extracted element is assigned the given id.
func ToAddress(rec []string, id string) []string {
	checkLength(rec)
	fields := []string{
		rec[AddressLine1Field],
		rec[AddressLine2Field],
		rec[CityField],
		rec[StateField],
		rec[ZipField],
	}
	if idfactor.None(fields...) {
		return nil
	}
	// Append empty Zip4
	fields = append(fields, "")
	return append([]string{rec[BreachIDField], id}, fields...)
}

// ToPhone extracts a phone identity element from the given full identity
// record. The extracted element is assigned the given id.
func ToPhone(rec []string, id string) []string {
	checkLength(rec)
	if idfactor.None(rec[PhoneField]) {
		return nil
	}
	return []string{rec[BreachIDField], id, rec[PhoneField]}
}

// ToEmail extracts an email identity element from the given full identity
// record. The extracted element is assigned the given id.
func ToEmail(rec []string, id string) []string {
	checkLength(rec)
	if idfactor.None(rec[EmailField]) {
		return nil
	}
	return []string{rec[BreachIDField], id, rec[EmailField]}
}

// ToNameAddress extracts a name and address identity element from the given
// full identity record. The extracted element is assigned the given id.
func ToNameAddress(rec []string, id string) []string {
	checkLength(rec)
	fields := []string{
		rec[FirstNameField],
		rec[LastNameField],
		rec[MiddleInitialField],
		rec[SuffixField],
		rec[AddressLine1Field],
		rec[AddressLine2Field],
		rec[CityField],
		rec[StateField],
		rec[ZipField],
	}
	if idfactor.None(fields...) {
		return nil
	}
	// Append empty Zip4 field
	fields = append(fields, "")
	return append([]string{rec[BreachIDField], id}, fields...)
}

// ToNamePhone extracts a name and phone identity element from the given full
// identity record. The extracted element is assigned the given id.
func ToNamePhone(rec []string, id string) []string {
	checkLength(rec)
	fields := []string{
		rec[FirstNameField],
		rec[LastNameField],
		rec[MiddleInitialField],
		rec[SuffixField],
		rec[PhoneField],
	}
	if idfactor.None(fields...) {
		return nil
	}
	return append([]string{rec[BreachIDField], id}, fields...)
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
	return idfactor.WriteToFile(recs, name, nameHeader, ToNameDob)
}

// WriteSsnFile extracts ssn identity elements from a list of full identity
// elements and writes them to the named file in shuffled order. It returns a
// map from record ids to element ids.
func WriteSsnFile(recs [][]string, name string) map[string]string {
	return idfactor.WriteToFile(recs, name, ssnHeader, ToSsn)
}

// WriteAddressFile extracts address identity elements from a list of full
// identity elements and writes them to the named file in shuffled order. It
// returns a map from record ids to element ids.
func WriteAddressFile(recs [][]string, name string) map[string]string {
	return idfactor.WriteToFile(recs, name, addressHeader, ToAddress)
}

// WritePhoneFile extracts phone identity elements from a list of full identity
// elements and writes them to the named file in shuffled order. It returns a
// map from record ids to element ids.
func WritePhoneFile(recs [][]string, name string) map[string]string {
	return idfactor.WriteToFile(recs, name, phoneHeader, ToPhone)
}

// WriteEmailFile extracts email identity elements from a list of full identity
// elements and writes them to the named file in shuffled order. It returns a
// map from record ids to element ids.
func WriteEmailFile(recs [][]string, name string) map[string]string {
	return idfactor.WriteToFile(recs, name, emailHeader, ToEmail)
}

// WriteNameAddressFile extracts name and address identity elements from a list
// of full identity elements and writes them to the named file in shuffled
// order. It returns a map from record ids to element ids.
func WriteNameAddressFile(recs [][]string, name string) map[string]string {
	return idfactor.WriteToFile(recs, name, nameAddressHeader, ToNameAddress)
}

// WriteNamePhoneFile extracts name and phone identity elements from a list of
// full identity elements and writes them to the named file in shuffled order.
// It returns a map from record ids to element ids.
func WriteNamePhoneFile(recs [][]string, name string) map[string]string {
	return idfactor.WriteToFile(recs, name, namePhoneHeader, ToNamePhone)
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
	return idfactor.WriteToWriter(recs, writer, nameHeader, ToNameDob)
}

// WriteSsn extracts ssn identity elements from a list of full identity
// elements and writes them to the given io.Writer in shuffled order. It returns
// a map from record ids to element ids.
func WriteSsn(recs [][]string, writer io.Writer) map[string]string {
	return idfactor.WriteToWriter(recs, writer, ssnHeader, ToSsn)
}

// WriteAddress extracts address identity elements from a list of full identity
// elements and writes them to the given io.Writer in shuffled order. It returns
// a map from record ids to element ids.
func WriteAddress(recs [][]string, writer io.Writer) map[string]string {
	return idfactor.WriteToWriter(recs, writer, addressHeader, ToAddress)
}

// WritePhone extracts phone identity elements from a list of full identity
// elements and writes them to the given io.Writer in shuffled order. It returns
// a map from record ids to element ids.
func WritePhone(recs [][]string, writer io.Writer) map[string]string {
	return idfactor.WriteToWriter(recs, writer, phoneHeader, ToPhone)
}

// WriteEmail extracts email identity elements from a list of full identity
// elements and writes them to the given io.Writer in shuffled order. It returns
// a map from record ids to element ids.
func WriteEmail(recs [][]string, writer io.Writer) map[string]string {
	return idfactor.WriteToWriter(recs, writer, emailHeader, ToEmail)
}

// WriteNameAddress extracts name and address identity elements from a list
// of full identity elements and writes them to the given io.Writer in shuffled
// order. It returns a map from record ids to element ids.
func WriteNameAddress(recs [][]string, writer io.Writer) map[string]string {
	return idfactor.WriteToWriter(recs, writer, nameAddressHeader, ToNameAddress)
}

// WriteNamePhone extracts name and phone identity elements from a list
// of full identity elements and writes them to the given io.Writer in shuffled
// order. It returns a map from record ids to element ids.
func WriteNamePhone(recs [][]string, writer io.Writer) map[string]string {
	return idfactor.WriteToWriter(recs, writer, namePhoneHeader, ToNamePhone)
}

//------------------------------------------------------------------------------
// compromised identity factoring
//------------------------------------------------------------------------------

func IDFactor(recs [][]string, dir string) ([][]string, error) {
	nameDobWriter := func(recs [][]string) map[string]string {
		return WriteNameDobFile(recs, filepath.Join(dir, "name_dob_elements.psv"))
	}
	ssnWriter := func(recs [][]string) map[string]string {
		return WriteSsnFile(recs, filepath.Join(dir, "ssn_elements.psv"))
	}
	addressWriter := func(recs [][]string) map[string]string {
		return WriteAddressFile(recs, filepath.Join(dir, "address_elements.psv"))
	}
	phoneWriter := func(recs [][]string) map[string]string {
		return WritePhoneFile(recs, filepath.Join(dir, "phone_elements.psv"))
	}
	emailWriter := func(recs [][]string) map[string]string {
		return WriteEmailFile(recs, filepath.Join(dir, "email_elements.psv"))
	}
	nameAddressWriter := func(recs [][]string) map[string]string {
		return WriteNameAddressFile(recs, filepath.Join(dir, "name_address_elements.psv"))
	}
	namePhoneWriter := func(recs [][]string) map[string]string {
		return WriteNamePhoneFile(recs, filepath.Join(dir, "name_phone_elements.psv"))
	}
	return idfactor.IDFactor(recs, nameDobWriter, ssnWriter, addressWriter, phoneWriter, emailWriter, nameAddressWriter, namePhoneWriter)
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
