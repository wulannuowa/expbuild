package file

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/expbuild/expbuild/pkg/util/random"
)

func RemoveIfExists(filename string) error {
	err := os.Remove(filename)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func EnsureDirectoryExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func WriteFile(fullPath string, data []byte) (int, error) {

	if err := EnsureDirectoryExists(filepath.Dir(fullPath)); err != nil {
		return 0, err
	}

	randStr := random.RandString(10)

	tmpFileName := fullPath + fmt.Sprintf(".%s.tmp", randStr)

	defer func() {
		RemoveIfExists(tmpFileName)
	}()

	if err := os.WriteFile(tmpFileName, data, 0644); err != nil {
		return 0, err
	}
	return len(data), os.Rename(tmpFileName, fullPath)
}

func ReadFile(fullPath string) ([]byte, error) {

	return os.ReadFile(fullPath)

}

func DeleteFile(fullPath string) error {
	return os.Remove(fullPath)
}

func FileExists(fullPath string) (bool, error) {
	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}
