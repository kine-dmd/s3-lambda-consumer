package main

import (
	"context"
	"encoding/binary"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/kine-dmd/s3-lambda-consumer/parquetHandler"
	"github.com/kine-dmd/s3-lambda-consumer/s3Connection"
	"log"
	"math"
	"os"
	"time"
)

type AppleWatch3Row struct {
	Ts uint64  `parquet:"name=ts, type=UINT_64"`
	Rx float64 `parquet:"name=rx, type=DOUBLE"`
	Ry float64 `parquet:"name=ry, type=DOUBLE"`
	Rz float64 `parquet:"name=rz, type=DOUBLE"`
	Rl float64 `parquet:"name=rl, type=DOUBLE"`
	Pt float64 `parquet:"name=pt, type=DOUBLE"`
	Yw float64 `parquet:"name=yw, type=DOUBLE"`
	Ax float64 `parquet:"name=ax, type=DOUBLE"`
	Ay float64 `parquet:"name=ay, type=DOUBLE"`
	Az float64 `parquet:"name=az, type=DOUBLE"`
	Hr float64 `parquet:"name=hr, type=DOUBLE"`
}

func main() {
	lambda.Start(lambdaMain)
}

func lambdaMain(_ context.Context, event events.S3Event) {
	// Extract the details and check we are dealing with a binary file
	bucketName, filePath := getFileLocation(event)
	if filePath[len(filePath)-4:] != ".bin" {
		return
	}

	// Make an S3 connection for donwloads and uploads
	s3Conn := s3Connection.MakeS3Connection()
	localBinaryFilePath := "/tmp/" + string(time.Now().UnixNano())
	_ = s3Conn.DownloadFile(bucketName, filePath, localBinaryFilePath)

	// Parse the data TODO: Refactor
	parsedData := readAndParseData(localBinaryFilePath)
	s := make([]interface{}, len(parsedData))
	for i, v := range parsedData {
		s[i] = v
	}

	// Make a parquet file to write the data to
	localParquetFilePath := "/tmp/" + string(time.Now().UnixNano())
	pqFile, _ := parquetHandler.MakeParquetFile(localParquetFilePath, new(AppleWatch3Row))
	_ = pqFile.WriteData(s)
	_ = pqFile.CloseFile()

	// Upload the file to other final S3 bucket
	f, _ := os.Open(localParquetFilePath)
	parquetFilePath := filePath[:len(filePath)-4] + ".parquet"
	_ = s3Conn.UploadFile("kine-dmd", parquetFilePath, f)

	// Delete the intermediary file
	_ = s3Conn.DeleteFile(bucketName, filePath)
}

func getFileLocation(event events.S3Event) (string, string) {
	bucketName := event.Records[0].S3.Bucket.Name
	filePath := event.Records[0].S3.Object.Key
	return bucketName, filePath
}

func readAndParseData(localFilePath string) []AppleWatch3Row {
	// Open the file and get size
	f, _ := os.Open(localFilePath)
	fileInfo, _ := f.Stat()
	defer f.Close()

	// Make a byte slice long enough for the entire thing
	rawData := make([]byte, fileInfo.Size())
	_, _ = f.Read(rawData)

	// Parse the data
	return decodeBinaryData(rawData)
}

func decodeBinaryData(raw []byte) []AppleWatch3Row {
	const bytesPerNumber int = 8
	const numFields int = 11
	const rowSize = bytesPerNumber * numFields

	// Check if there are an integer number of rows
	if len(raw)%rowSize != 0 {
		log.Println("Last row of binary data may be corrupted.")
	}

	// Calculate number of rows to be read
	var n int = len(raw) / rowSize
	rows := make([]AppleWatch3Row, n)

	// Parse each row
	offset := 0
	for i := 0; i < n; i++ {

		// Store the numbers as an intermediary uint64
		nums := make([]uint64, numFields)

		// Read each row in the field
		for j := 0; j < numFields; j++ {
			nums[j] = binary.LittleEndian.Uint64(raw[offset : offset+bytesPerNumber])
			offset += bytesPerNumber
		}

		// Convert to floats and put inside struct
		rows[i] = AppleWatch3Row{
			nums[0],
			math.Float64frombits(nums[1]),
			math.Float64frombits(nums[2]),
			math.Float64frombits(nums[3]),
			math.Float64frombits(nums[4]),
			math.Float64frombits(nums[5]),
			math.Float64frombits(nums[6]),
			math.Float64frombits(nums[7]),
			math.Float64frombits(nums[8]),
			math.Float64frombits(nums[9]),
			math.Float64frombits(nums[10]),
		}
	}
	return rows
}
