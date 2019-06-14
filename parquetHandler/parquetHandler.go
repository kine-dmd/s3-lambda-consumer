package parquetHandler

import (
	"bytes"
	"github.com/kine-dmd/s3-lambda-consumer/appleWatch3Row"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
	"log"
	"runtime"
)

func ConvertToParquetFile(allData []appleWatch3Row.AppleWatch3Row) ([]byte, error) {
	// Create an in memory parquet file
	inMem := makeInMemoryParquetFile()

	// Create a file writer for that file
	cpuThreads := int64(runtime.NumCPU())
	parquetWriter, err := ParquetWriter.NewParquetWriter(inMem, new(appleWatch3Row.AppleWatch3Row), cpuThreads)
	if err != nil {
		log.Println("Unable to create parquet writer ", err)
		return nil, err
	}

	// Use default row group size and compression codecs
	parquetWriter.RowGroupSize = 128 * 1024 * 1024 // 128 MB
	parquetWriter.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write each row to the file
	for _, row := range allData {
		err := parquetWriter.Write(row)
		if err != nil {
			log.Println("Error writing row to file ", err, row)
			return nil, err
		}
	}

	// Write footer to parquet file
	err = parquetWriter.WriteStop()
	if err != nil {
		log.Println("Unable to write footer to Parquet file ", err)
		return nil, err
	}

	// Success - no error to return
	return inMem.getData(), nil
}

/** An in memory parquet file to avoid writing to disk. Manually implements methods not provided by composition. **/
type inMemoryParquetFile struct {
	data   []byte
	reader *bytes.Reader
	*bytes.Reader
	*bytes.Buffer
}

func makeInMemoryParquetFile() inMemoryParquetFile {
	inMemFile := inMemoryParquetFile{}
	inMemFile.data = []byte{}
	inMemFile.reader = bytes.NewReader(inMemFile.data)
	inMemFile.Reader = inMemFile.reader
	inMemFile.Buffer = bytes.NewBuffer(inMemFile.data)
	return inMemFile
}

func (pqf inMemoryParquetFile) Open(name string) (ParquetFile.ParquetFile, error) {
	return pqf, nil
}

func (pqf inMemoryParquetFile) Read(b []byte) (int, error) {
	return pqf.reader.Read(b)
}

func (pqf inMemoryParquetFile) Create(name string) (ParquetFile.ParquetFile, error) {
	return pqf, nil
}

func (pqf inMemoryParquetFile) Close() error {
	return nil
}

func (pqf inMemoryParquetFile) getData() []byte {
	return pqf.Buffer.Bytes()
}
