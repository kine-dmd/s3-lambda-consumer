package s3Connection

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"
)

const (
	TEST_BUCKET_NAME = "kine-dmd-test"
)

func TestS3Cycle(t *testing.T) {
	// Make the connection, generate a random file name and random data
	s3Conn := MakeS3Connection()
	filename := strconv.FormatUint(rand.Uint64(), 10)
	originalData := makeRandomData(10000000)

	// Upload the data
	err := s3Conn.UploadFile(TEST_BUCKET_NAME, filename, bytes.NewReader(originalData))
	if err != nil {
		t.Fatalf("Error uploading file to S3: %s", err)
	}

	// Try and download the data and compare it
	downloadedData, err := s3Conn.DownloadFileToMemory(TEST_BUCKET_NAME, filename)
	if err != nil {
		t.Fatalf("Error downloading file from S3: %s", err)
	}
	compareData(t, originalData, downloadedData)

	// Try and delete the file
	err = s3Conn.DeleteFile(TEST_BUCKET_NAME, filename)
	if err != nil {
		t.Fatalf("Error deleting file from S3: %s", err)
	}

	// Try to re-download the file to make sure it was actually deleted
	_, err = s3Conn.DownloadFileToMemory(TEST_BUCKET_NAME, filename)
	if err == nil {
		t.Fatalf("No error thrown when attempting to delete non-existant file.")
	}
}

func compareData(t *testing.T, expected []byte, actual []byte) {
	if len(expected) != len(actual) {
		t.Fatalf("Expected result length not equal to actual. Expected length %d. Got length %d.", len(expected), len(actual))
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Byte %d does not match. Expected %b, got %b", i, expected[i], actual[i])
		}
	}
}

func makeRandomData(length int) []byte {
	data := make([]byte, length)
	rand.Read(data)
	return data
}
