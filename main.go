package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/kine-dmd/s3-lambda-consumer/appleWatch3Row"
	"github.com/kine-dmd/s3-lambda-consumer/parquetHandler"
	"github.com/kine-dmd/s3-lambda-consumer/s3Connection"
	"log"
	"math"
	"runtime"
)

const (
	bytesPerNumber int = 8
	numFields      int = 11
	rowSize            = bytesPerNumber * numFields
)

func main() {
	lambda.Start(lambdaMain)
}

func lambdaMain(_ context.Context, event events.S3Event) {
	// Extract the details and check we are dealing with a binary file
	bucketName, filePath := getFileLocation(event)
	if filePath[len(filePath)-4:] != ".bin" {
		return
	}

	// Make an S3 connection for downloads and uploads
	s3Conn := s3Connection.MakeS3Connection()
	binaryData, err := s3Conn.DownloadFileToMemory(bucketName, filePath)
	if err != nil {
		log.Fatalf("Unable to download file %s from S3: %s", filePath, err)
	}

	// Parse the binaryData and then convert it to parquet
	parsedData := decodeBinaryData(binaryData)
	parquetData, err := parquetHandler.ConvertToParquetFile(parsedData, runtime.NumCPU())
	if err != nil {
		log.Fatalf("Unable to convert file %s to parquet: %s", filePath, err)
	}

	// Strip the .bin extension and replace with .parquet and upload file
	parquetFilePath := filePath[:len(filePath)-4] + ".parquet"
	err = s3Conn.UploadFile("kine-dmd", parquetFilePath, bytes.NewReader(parquetData))
	if err != nil {
		log.Fatalf("Unable to uploaded converted file %s to S3: %s", parquetFilePath, err)
	}

	// Delete the intermediary file from the S3 bucket
	err = s3Conn.DeleteFile(bucketName, filePath)
	if err != nil {
		log.Printf("Unable to remove intermediary file %s from S#: %s", parquetFilePath, err)
	}
}

func getFileLocation(event events.S3Event) (string, string) {
	// Extract the bucket name and file path from the lambda event object that it is invoked with
	bucketName := event.Records[0].S3.Bucket.Name
	filePath := event.Records[0].S3.Object.Key
	return bucketName, filePath
}

func decodeBinaryData(raw []byte) []appleWatch3Row.AppleWatch3Row {
	// Check if there are an integer number of rows
	if len(raw)%rowSize != 0 {
		log.Fatalf("Binary data is corrupted.")
	}

	// Calculate number of rows to be read
	var numRows int = len(raw) / rowSize
	rows := make([]appleWatch3Row.AppleWatch3Row, numRows)

	// Parse each row
	offset := 0
	for i := 0; i < numRows; i++ {

		// Store the numbers as an intermediary uint64
		nums := make([]uint64, numFields)

		// Read each row in the field
		for j := 0; j < numFields; j++ {
			nums[j] = binary.LittleEndian.Uint64(raw[offset : offset+bytesPerNumber])
			offset += bytesPerNumber
		}

		// Convert to floats and put inside struct
		rows[i] = appleWatch3Row.AppleWatch3Row{
			Ts: nums[0],
			Rx: math.Float64frombits(nums[1]),
			Ry: math.Float64frombits(nums[2]),
			Rz: math.Float64frombits(nums[3]),
			Rl: math.Float64frombits(nums[4]),
			Pt: math.Float64frombits(nums[5]),
			Yw: math.Float64frombits(nums[6]),
			Ax: math.Float64frombits(nums[7]),
			Ay: math.Float64frombits(nums[8]),
			Az: math.Float64frombits(nums[9]),
			Hr: math.Float64frombits(nums[10]),
		}
	}
	return rows
}
