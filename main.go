package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	bucketName = "zhongyi-multipart-test"
	keyName    = "seq001"
	regionName = "us-west-1"
)

func main() {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region: aws.String(regionName),
		}))
	client := s3.New(sess)

	// File body:
	// 1111111111...
	// 2222222222...
	// 3333333333...
	// 4444444444...
	upload(client)
	download(client)
}

func upload(client *s3.S3) {
	// Test if the parts can be uploaded in random order
	initRes, initErr :=
		client.CreateMultipartUpload(
			&s3.CreateMultipartUploadInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(keyName),
			})
	checkErr(initErr)
	fmt.Println("Upload ID: ", initRes.UploadId)

	completedParts := make([]*s3.CompletedPart, 4)

	uploadPart(client, 2, initRes.UploadId, completedParts[:])
	uploadPart(client, 3, initRes.UploadId, completedParts[:])
	uploadPart(client, 4, initRes.UploadId, completedParts[:])
	uploadPart(client, 1, initRes.UploadId, completedParts[:])

	completeRes, completeErr := client.CompleteMultipartUpload(
		&s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucketName),
			Key:             aws.String(keyName),
			UploadId:        initRes.UploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
		})

	checkErr(completeErr)
	fmt.Println(completeRes)
}

func download(client *s3.S3) {
	// Test if the parts can be downloaded with original PartId
	downloadPart(client, 1)
	downloadPart(client, 3)
	downloadPart(client, 2)
	downloadPart(client, 4)
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func generatePart(partId int) *bytes.Reader {
	body := strings.Repeat(strconv.Itoa(partId), 1024*1024*5) + "\n"
	return bytes.NewReader([]byte(body))
}

func uploadPart(client *s3.S3, partId int, uploadId *string, completedParts []*s3.CompletedPart) {
	uploadRequest := s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(keyName),
		UploadId:   uploadId,
		PartNumber: aws.Int64(int64(partId)),
		Body:       generatePart(partId),
	}

	partRes, _ := client.UploadPart(&uploadRequest)
	completedParts[partId-1] = &s3.CompletedPart{
		ETag:       partRes.ETag,
		PartNumber: aws.Int64(int64(partId)),
	}
}

func downloadPart(client *s3.S3, partId int) {
	res, err := client.GetObject(&s3.GetObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(keyName),
		PartNumber: aws.Int64(int64(partId)),
	})
	checkErr(err)

	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Body)
	fmt.Println(buf.String()[:1])
}
