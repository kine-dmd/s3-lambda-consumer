package parquetHandler

import (
	"github.com/kine-dmd/s3-lambda-consumer/appleWatch3Row"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
	"log"
	"runtime"
)

type ParquetFileHandler struct {
	file   ParquetFile.ParquetFile
	writer *ParquetWriter.ParquetWriter
}

func MakeParquetFile(filePath string) (*ParquetFileHandler, error) {
	// Create a file to write to
	fileWriter, err := ParquetFile.NewLocalFileWriter(filePath)
	if err != nil {
		log.Println("Unable to create parquet file ", err)
		return nil, err
	}

	// Create a file writer for that file
	cpuThreads := int64(runtime.NumCPU())
	parquetWriter, err := ParquetWriter.NewParquetWriter(fileWriter, new(appleWatch3Row.AppleWatch3Row), cpuThreads)
	if err != nil {
		log.Println("Unable to create parquet writer ", err)
		return nil, err
	}

	// Use default row group size and compression codecs
	parquetWriter.RowGroupSize = 128 * 1024 * 1024 // 128 MB
	parquetWriter.CompressionType = parquet.CompressionCodec_SNAPPY

	// Save for use
	parqFile := new(ParquetFileHandler)
	parqFile.writer = parquetWriter
	parqFile.file = fileWriter

	// No errors to return
	return parqFile, nil
}

func (parqFileHandler *ParquetFileHandler) WriteData(allData []appleWatch3Row.AppleWatch3Row) error {
	// Write each row to the file
	for _, row := range allData {
		err := parqFileHandler.writer.Write(row)
		if err != nil {
			log.Println("Error writing row to file ", err, row)
			return err
		}
	}

	// Success - no error to return
	return nil
}

func (parqFileHandler *ParquetFileHandler) CloseFile() error {
	// Write footer to parquet file
	err := parqFileHandler.writer.WriteStop()
	if err != nil {
		log.Println("Unable to write footer to Parquet file ", err)
		return err
	}

	// Close the file itself
	err = parqFileHandler.file.Close()
	if err != nil {
		log.Println("Unable to close Parquet file ", err)
		return err
	}

	// Success - no error to return
	return nil
}
