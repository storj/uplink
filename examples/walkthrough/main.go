// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"storj.io/uplink"
	"storj.io/uplink/edge"
)

const defaultExpiration = 7 * 24 * time.Hour

func CreateProjectAndConfirmBucket(ctx context.Context, accessGrant, bucketName string) (*uplink.Project, error) {
	// Parse access grant, which contains necessary credentials and permissions.
	access, err := uplink.ParseAccess(accessGrant)
	if err != nil {
		return nil, fmt.Errorf("could not request access grant: %v", err)
	}

	// Open up the Project we will be working with.
	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, fmt.Errorf("could not open project: %v", err)
	}

	// Ensure the desired Bucket within the Project is created.
	_, err = project.EnsureBucket(ctx, bucketName)
	if err != nil {
		_ = project.Close()
		return nil, fmt.Errorf("could not ensure bucket: %v", err)
	}

	return project, nil
}

// UploadAndDownloadData uploads the specified data to the specified key in the
// specified bucket, using the specified Satellite, API key, and passphrase.
func UploadAndDownloadData(ctx context.Context,
	accessGrant, bucketName, uploadKey string, dataToUpload []byte) error {

	// Open up the Project we will be working with.
	project, err := CreateProjectAndConfirmBucket(ctx, accessGrant, bucketName)
	if err != nil {
		return fmt.Errorf("could not create project: %v", err)
	}
	defer project.Close()

	// Intitiate the upload of our Object to the specified bucket and key.
	upload, err := project.UploadObject(ctx, bucketName, uploadKey, &uplink.UploadOptions{
		// It's possible to set an expiration date for data.
		Expires: time.Now().Add(defaultExpiration),
	})
	if err != nil {
		return fmt.Errorf("could not initiate upload: %v", err)
	}

	// Copy the data to the upload.
	buf := bytes.NewBuffer(dataToUpload)
	_, err = io.Copy(upload, buf)
	if err != nil {
		_ = upload.Abort()
		return fmt.Errorf("could not upload data: %v", err)
	}

	// Commit the uploaded object.
	err = upload.Commit()
	if err != nil {
		return fmt.Errorf("could not commit uploaded object: %v", err)
	}

	// Initiate a download of the same object again
	download, err := project.DownloadObject(ctx, bucketName, uploadKey, nil)
	if err != nil {
		return fmt.Errorf("could not open object: %v", err)
	}
	defer download.Close()

	// Read everything from the download stream
	receivedContents, err := io.ReadAll(download)
	if err != nil {
		return fmt.Errorf("could not read data: %v", err)
	}

	// Check that the downloaded data is the same as the uploaded data.
	if !bytes.Equal(receivedContents, dataToUpload) {
		return fmt.Errorf("got different object back: %q != %q", dataToUpload, receivedContents)
	}

	return nil
}

func CreatePublicSharedLink(ctx context.Context, accessGrant, bucketName, objectKey string) (string, error) {
	// Define configuration for the storj sharing site.
	config := edge.Config{
		AuthServiceAddress: "auth.storjshare.io:7777",
	}

	// Parse access grant, which contains necessary credentials and permissions.
	access, err := uplink.ParseAccess(accessGrant)
	if err != nil {
		return "", fmt.Errorf("could not parse access grant: %w", err)
	}

	// Restrict access to the specified paths.
	restrictedAccess, err := access.Share(
		uplink.Permission{
			// only allow downloads
			AllowDownload: true,
			// this allows to automatically cleanup the access grants
			NotAfter: time.Now().Add(defaultExpiration),
		}, uplink.SharePrefix{
			Bucket: bucketName,
			Prefix: objectKey,
		})
	if err != nil {
		return "", fmt.Errorf("could not restrict access grant: %w", err)
	}

	// RegisterAccess registers the credentials to the linksharing and s3 sites.
	// This makes the data publicly accessible, see the security implications in https://docs.storj.io/dcs/concepts/access/access-management-at-the-edge.
	credentials, err := config.RegisterAccess(ctx, restrictedAccess, &edge.RegisterAccessOptions{Public: true})
	if err != nil {
		return "", fmt.Errorf("could not register access: %w", err)
	}

	// Create a public link that is served by linksharing service.
	url, err := edge.JoinShareURL("https://link.storjshare.io", credentials.AccessKeyID, bucketName, objectKey, nil)
	if err != nil {
		return "", fmt.Errorf("could not create a shared link: %w", err)
	}

	return url, nil
}

func MultipartUpload(ctx context.Context, accessGrant, bucketName, objectKey string, contents1, contents2 []byte) error {
	// Create the project and ensure the bucket exists
	project, err := CreateProjectAndConfirmBucket(ctx, accessGrant, bucketName)
	if err != nil {
		return fmt.Errorf("could not create project: %v", err)
	}
	defer project.Close()

	// Create multipart upload
	uploadInfo, err := project.BeginUpload(ctx, bucketName, objectKey, &uplink.UploadOptions{
		// It's possible to set an expiration date for data.
		Expires: time.Now().Add(defaultExpiration),
	})

	if err != nil {
		return fmt.Errorf("unable to begin multipart upload: %v", err)
	}

	// Start the upload of part 1
	part1, err := project.UploadPart(ctx, bucketName, objectKey, uploadInfo.UploadID, 1)
	if err != nil {
		return fmt.Errorf("unable to upload part 1: %v", err)
	}

	// Write the contents to part 1 upload
	_, err = part1.Write(contents1)
	if err != nil {
		return fmt.Errorf("unable to write to part 1: %v", err)
	}

	// Commit part 1 upload
	err = part1.Commit()
	if err != nil {
		return fmt.Errorf("unable to commit to part 1: %v", err)
	}

	// Start the upload of part 2
	part2, err := project.UploadPart(ctx, bucketName, objectKey, uploadInfo.UploadID, 2)
	if err != nil {
		return fmt.Errorf("unable to upload part 2: %v", err)
	}

	// Write the contents to part 2 upload
	_, err = part2.Write(contents2)
	if err != nil {
		return fmt.Errorf("unable to write to part 2: %v", err)
	}

	// Commit part 2 upload
	err = part2.Commit()
	if err != nil {
		return fmt.Errorf("unable to commit to part 2: %v", err)
	}

	// Complete the multipart upload
	multipartObject, err := project.CommitUpload(ctx, bucketName, objectKey, uploadInfo.UploadID, nil)
	if err != nil {
		return fmt.Errorf("unable to complete the upload: %v", err)
	}

	// Initiate a download of the same object again
	download, err := project.DownloadObject(ctx, bucketName, objectKey, nil)
	if err != nil {
		return fmt.Errorf("could not open multipart object: %v", err)
	}
	defer download.Close()

	// Read everything from the download stream
	receivedContents, err := io.ReadAll(download)
	if err != nil {
		return fmt.Errorf("could not read multipart data: %v", err)
	}

	// Check that the downloaded data is the same as the uploaded data.
	uploadedData := append(contents1, contents2...)
	if !bytes.Equal(receivedContents, uploadedData) {
		return fmt.Errorf("got different multipart object back: %q != %q", uploadedData, receivedContents)
	}

	fmt.Fprintln(os.Stderr, "success!")
	fmt.Fprintln(os.Stderr, "completed multipart upload:", multipartObject.Key)

	return nil
}

func createMultipartData() (contents1, contents2 []byte) {
	const minimumUploadSize = 5 * 1024 * 1024 // 5 MiB

	// The first contents is the alphabet on repeat
	alphabet := "abcdefghijklmnopqrstuvwxyz"

	contents1 = make([]byte, minimumUploadSize)
	alphabetLength := len(alphabet)
	for i := 0; i < minimumUploadSize; i++ {
		contents1[i] = alphabet[i%alphabetLength]
	}

	// The second contents is numbers on repeat
	numbers := "1234567890"
	numbersLength := len(numbers)
	contents2 = make([]byte, minimumUploadSize)
	for i := 0; i < minimumUploadSize; i++ {
		contents2[i] = numbers[i%numbersLength]
	}

	return contents1, contents2
}

func main() {
	ctx := context.Background()
	accessGrant := flag.String("access", os.Getenv("ACCESS_GRANT"), "access grant from satellite")
	flag.Parse()

	bucketName := "my-first-bucket"
	objectKey := "foo/bar/baz"

	err := UploadAndDownloadData(ctx, *accessGrant, bucketName, objectKey, []byte("one fish two fish red fish blue fish"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "upload failed:", err)
		os.Exit(1)
	}

	url, err := CreatePublicSharedLink(ctx, *accessGrant, bucketName, objectKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, "creating public link failed:", err)
		os.Exit(1)
	}

	fmt.Println("success!")
	fmt.Println("public link:", url)

	largeObjectKey := "foo/bar/large"

	largeContents1, largeContents2 := createMultipartData()

	err = MultipartUpload(ctx, *accessGrant, bucketName, largeObjectKey, largeContents1, largeContents2)
	if err != nil {
		fmt.Fprintln(os.Stderr, "creating mulipart upload failed:", err)
		os.Exit(1)
	}
}
