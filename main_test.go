package main

import (
	"encoding/binary"
	"github.com/aws/aws-lambda-go/events"
	"github.com/kine-dmd/s3-lambda-consumer/appleWatch3Row"
	"math"
	"math/rand"
	"testing"
)

func TestUnpackFileLocation(t *testing.T) {
	// Create a dummy event as per the S3 spec
	const bucket, key = "test-bucket", "test-key"
	event := events.S3Event{
		[]events.S3EventRecord{{S3: events.S3Entity{
			Bucket: events.S3Bucket{Name: bucket},
			Object: events.S3Object{Key: key},
		}}},
	}

	// Try and unpack the data
	unpacked_bucket, unpacked_key := getFileLocation(event)

	// Check data matches
	if unpacked_bucket != bucket {
		t.Fatalf("Retrieved wrong bucket id. Expected %s. Got %s.", bucket, unpacked_bucket)
	}
	if unpacked_key != key {
		t.Fatalf("Retrieved wrong object id. Expected %s. Got %s.", key, unpacked_key)
	}

}

func TestDecoding1Row(t *testing.T) {
	testDecodingRows(t, 1)
}

func TestDecoding100Rows(t *testing.T) {
	testDecodingRows(t, 100)
}

func TestDecoding10000Rows(t *testing.T) {
	testDecodingRows(t, 10000)
}

func testDecodingRows(t *testing.T, numRows int) {
	// Make some fake rows and encode it
	rows := MakeRandomRows(numRows)
	binaryData := encodeWatchRows(rows)

	// Try and decode the data
	decodedRows := decodeBinaryData(binaryData)

	// Check the decoded data matches
	if len(rows) != len(decodedRows) {
		t.Fatalf("Not same number of rows decoded. Expected %d. Got %d.", len(rows), len(decodedRows))
	}
	for i := range rows {
		if rows[i] != decodedRows[i] {
			t.Fatalf("Row %d does not match. Expected %+v. Got %+v.", i, rows[i], decodedRows[i])
		}
	}
}

/** Helper functions **/

func encodeWatchRows(watchRows []appleWatch3Row.AppleWatch3Row) []byte {

	var encodedData []byte
	for _, row := range watchRows {
		encodedData = append(encodedData, encodeRow(row)...)
	}

	return encodedData

}

func encodeRow(row appleWatch3Row.AppleWatch3Row) []byte {
	// Make enough space for one row
	data := make([]byte, rowSize)

	// Convert each number to binary and store at the appropriate offset
	binary.LittleEndian.PutUint64(data, row.Ts)
	binary.LittleEndian.PutUint64(data[bytesPerNumber*1:], math.Float64bits(row.Rx))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*2:], math.Float64bits(row.Ry))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*3:], math.Float64bits(row.Rz))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*4:], math.Float64bits(row.Rl))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*5:], math.Float64bits(row.Pt))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*6:], math.Float64bits(row.Yw))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*7:], math.Float64bits(row.Ax))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*8:], math.Float64bits(row.Ay))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*9:], math.Float64bits(row.Az))
	binary.LittleEndian.PutUint64(data[bytesPerNumber*10:], math.Float64bits(row.Hr))
	return data
}

func MakeRandomRows(n int) []appleWatch3Row.AppleWatch3Row {
	aw3rows := make([]appleWatch3Row.AppleWatch3Row, n)
	for i := range aw3rows {
		aw3rows[i] = MakeRandomRow()
	}
	return aw3rows
}

func MakeRandomRow() appleWatch3Row.AppleWatch3Row {
	return appleWatch3Row.AppleWatch3Row{
		Ts: rand.Uint64(),
		Rx: rand.Float64(),
		Ry: rand.Float64(),
		Rz: rand.Float64(),
		Rl: rand.Float64(),
		Pt: rand.Float64(),
		Yw: rand.Float64(),
		Ax: rand.Float64(),
		Ay: rand.Float64(),
		Az: rand.Float64(),
		Hr: rand.Float64(),
	}
}
