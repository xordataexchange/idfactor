package idfactor

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"runtime"
	"sync"

	"xor/lib/shuffle"
	"xor/lib/uuid"
)

const recordIDField = 0

//------------------------------------------------------------------------------
// These functions write out an element ID map to a file or io.Writer.
//------------------------------------------------------------------------------

// WriteMapToFile writes an element id map to the file with the given name.
func WriteMapToFile(ids [][]string, name string) {
	file, err := os.Create(name)
	if err != nil {
		log.Fatalf(`idfactor: error creating file "%s": %s`, name, err)
	}
	WriteMapToWriter(ids, file)
	if err := file.Close(); err != nil {
		log.Fatalf(`idfactor: error closing file "%s": %s`, name, err)
	}
}

// WriteMapToWriter writers an element id map to the given io.Writer.
func WriteMapToWriter(ids [][]string, w io.Writer) {
	writer := csv.NewWriter(w)
	writer.Comma = '|'
	// line terminator
	if runtime.GOOS == "windows" {
		writer.UseCRLF = true
	}
	// write file header
	var mapHeader = []string{"record_id", "name_id", "ssn_id", "address_id", "phone_id", "email_id", "name_address_id", "name_phone_id"}
	if err := writer.Write(mapHeader); err != nil {
		log.Fatalf("idfactor: error writing file: %s", err)
	}
	if err := writer.WriteAll(ids); err != nil {
		log.Fatalf("idfactor: error writing file: %s", err)
	}
}

//------------------------------------------------------------------------------
// These functions extract a single identity element from a list of records and
// writes them out to a file or io.Writer in shuffled order.
//------------------------------------------------------------------------------

// ElementGetter is the function signature of functions that extract an
// identity element from a full identity record.
type ElementGetter func(rec []string, id string) []string

// WriteToFile extracts identity elements from a list of full identity records
// and writes them to the named file. It returns a map from record ids to
// element ids.
func WriteToFile(recs [][]string, name string, header []string, get ElementGetter) map[string]string {
	file, err := os.Create(name)
	if err != nil {
		log.Fatalf(`idfactor: error creating file "%s": %s`, name, err)
	}
	result := WriteToWriter(recs, file, header, get)
	if err := file.Close(); err != nil {
		log.Fatalf(`idfactor: error closing file "%s": %s`, name, err)
	}
	return result
}

// WriteToWriter extracts identity elements from a list of full identity records
// and writes them to the given io.Writer. It returns a map from record ids to
// element ids.
func WriteToWriter(recs [][]string, w io.Writer, header []string, get ElementGetter) map[string]string {
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
		id := recs[i][recordIDField]
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
//  This function applies a list of functions to a list of records.
//------------------------------------------------------------------------------

type Factorer func(recs [][]string) map[string]string

func IDFactor(recs [][]string, factorers ...Factorer) ([][]string, error) {
	n := len(factorers)
	idMaps := make([]map[string]string, n)

	// concurrent factoring
	workers := sync.WaitGroup{}
	for i, factor := range factorers {
		workers.Add(1)
		go func(i int, factor Factorer) {
			idMaps[i] = factor(recs)
			workers.Done()
		}(i, factor)
	}
	workers.Wait()

	// construct id map
	ids := make([][]string, len(recs))
	for i := range recs {
		recordID := recs[i][recordIDField]
		ids[i] = make([]string, 1+n)
		ids[i][0] = recordID
		for j := range idMaps {
			ids[i][1+j] = idMaps[j][recordID]
		}
	}

	return ids, nil
}

//------------------------------------------------------------------------------
// utility functions
//------------------------------------------------------------------------------

// AllEmpty returns true if and only if no nonempty strings are supplied
func AllEmpty(strs ...string) bool {
	for _, s := range strs {
		if s != "" {
			return false
		}
	}
	return true
}
