package file

import (
	"io"
	"log"
	"os"
)

// UploadFile uploads a file to the specified destination.
func UploadFile(srcPath, destPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return err
	}

	log.Printf("File uploaded successfully: %s", destPath)
	return nil
}

// DownloadFile downloads a file from the specified source.
func DownloadFile(srcPath, destPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return err
	}

	log.Printf("File downloaded successfully: %s", srcPath)
	return nil
}
