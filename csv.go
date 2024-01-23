package gocortado

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

type CsvLoader2 struct {
	Path   string
	File   *os.File
	Gz     *gzip.Reader
	Csv    *csv.Reader
	Header []string
}

// detects .gz suffix and reads using gunzip. if
// path is "-" we read from stdin
func NewCsvLoader2(path string) (*CsvLoader2, error) {

	s := &CsvLoader2{
		Path: path,
	}

	var f *os.File
	var err error
	if path == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(s.Path)
		if err != nil {
			return nil, err
		}
	}
	s.File = f

	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		s.Gz = gz
		s.Csv = csv.NewReader(s.Gz)
	} else {
		s.Gz = nil
		s.Csv = csv.NewReader(s.File)
	}

	head, err := s.ReadOne()
	if err != nil {
		return nil, fmt.Errorf("could not read header from path '%s': '%s'", path, err)
	}
	s.Header = head

	return s, nil
}

func (s *CsvLoader2) Close() error {
	var err, err2 error
	if s.Gz != nil {
		err = s.Gz.Close()
	}
	err2 = s.File.Close()
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func (s *CsvLoader2) ReadOne() ([]string, error) {
	return s.Csv.Read() // Read() to get one record.
}

func usageCsvShow() {
	fmt.Fprintf(os.Stderr, "showcvs <path to .csv{.gz} to display or - for stdin> {-json} {-jsonpp} {-header <pull header from this file; useful with - for stdin input>}\n")
	os.Exit(1)
}

func present(needle string, haystack []string) (present bool, position int) {
	for i := range haystack {
		if haystack[i] == needle {
			return true, i
		}
	}
	return false, -1
}

func CsvShowMain() {

	defer os.Stdout.Sync()
	if len(os.Args) < 2 {
		usageCsvShow()
	}
	fn := os.Args[1]
	if fn == "-" {
		// don't check file
	} else if !FileExists(fn) {
		fmt.Fprintf(os.Stderr, "error: inspecting '%s' impossible as it does not exist.", fn)
		os.Exit(1)
	}

	pretty := ""
	json, _ := present("-json", os.Args)

	jsonpp, _ := present("-jsonpp", os.Args)
	if jsonpp {
		// jsonpp for pretty print
		json = true
		pretty = "\n"
	}

	headerFrom := ""
	headerPresent, headerpos := present("-header", os.Args)
	var headerCsv *CsvLoader2
	var err error
	if headerPresent {
		if headerpos+1 >= len(os.Args) {
			fmt.Fprintf(os.Stderr, "error: -header must be "+
				"followed by file path to take header from.")
			os.Exit(1)
		}

		headerFrom = os.Args[headerpos+1]
		//fmt.Printf("\n using headerFrom = '%s'\n", headerFrom)
		if !FileExists(headerFrom) {
			fmt.Fprintf(os.Stderr, "error: -header from '%s' failed "+
				"as that path is not a existing file.", headerFrom)
			os.Exit(1)
		}

		headerCsv, err = NewCsvLoader2(headerFrom)
		panicOn(err)
		headerCsv.Close()
	}

	// load csv
	cl, err := NewCsvLoader2(fn)
	panicOn(err)

	if headerCsv != nil {
		cl.Header = headerCsv.Header
	}

	// stream all
	i := 1
	if json {
		for {
			fmt.Printf("{%s", pretty)

			rec, err := cl.ReadOne()
			if err != nil {
				break
			}
			comma := ","
			lastrec := len(rec) - 1
			for j, r := range rec {
				if r != "" {
					if j == lastrec {
						comma = ""
					}
					fmt.Printf(" \"%s\": \"%s\"%s%s", cl.Header[j], r, comma, pretty)
				}
			}
			fmt.Printf("}\n")
		}
	} else {
		for {
			fmt.Printf("=========== begin %d\n", i)

			rec, err := cl.ReadOne()
			if err != nil {
				break
			}
			//fmt.Printf("rec = '%#v'\n", rec)
			for j, r := range rec {
				if r != "" {
					fmt.Printf("   [%d] '%s': '%s'\n", j, cl.Header[j], r)
				}
			}
			fmt.Printf("=========== end %d\n", i)
			i++
		}
	}
}
