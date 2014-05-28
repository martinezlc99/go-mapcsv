package mapcsv

import (
	"encoding/csv"
	"fmt"
	"io"
)

type MapReader struct {
	fields  []string       //fieldnames for csv file
	mapping map[string]int //map of fieldnames to column number
	*csv.Reader
}

type MapWriter struct {
	fields  []string       //fieldnames for csv file
	mapping map[string]int //map of fieldnames to column number
	*csv.Writer
}

func (r *MapReader) GetFieldNames() []string {
	return r.fields
}

func (r *MapReader) Read() (maprecord map[string]string, err error) {
	record, err := r.Reader.Read()
	if err != nil {
		return nil, err
	}
	maprecord = make(map[string]string)
	for k, v := range r.mapping {
		maprecord[k] = record[v]
	}
	return maprecord, nil
}

func (r *MapReader) ReadAll() (maprecords []map[string]string, err error) {
	maprecords = make([]map[string]string, 0)
	for {
		maprecord, err := r.Read()
		if err == io.EOF {
			return maprecords, nil
		}
		if err != nil {
			return nil, err
		}
		maprecords = append(maprecords, maprecord)
	}
}

func (w *MapWriter) WriteFieldNames() {
	w.Writer.Write(w.fields)
}

func (w *MapWriter) Write(maprecord map[string]string) (err error) {
	record := make([]string, len(w.fields))
	for k, v := range maprecord {
		if column, ok := w.mapping[k]; ok {
			record[column] = v
		} else {
			return fmt.Errorf("%s is not a valid field name.  Valid field names are %s", k, w.fields)
		}
	}
	w.Writer.Write(record)
	return nil
}

func (w *MapWriter) WriteAll(maprecords []map[string]string) (err error) {
    for _, maprecord := range maprecords {
        err = w.Write(maprecord)
        if err != nil {
           return err
        }
    }
    w.Writer.Flush()
	return nil
}

func NewMapReader(r io.Reader) (*MapReader, error) {
	reader := csv.NewReader(r)
	mapping := make(map[string]int)
	//read first row to get header information
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	for i := range header {
		mapping[header[i]] = i
	}
	return &MapReader{header, mapping, reader}, nil
}

func NewMapWriter(w io.Writer, fields []string) (*MapWriter, error) {
	writer := csv.NewWriter(w)
	mapping := make(map[string]int)
	for i := range fields {
		mapping[fields[i]] = i
	}
	return &MapWriter{fields, mapping, writer}, nil
}