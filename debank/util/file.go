package util

import "os"

func WriteFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0600)
}

func ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func RemoveDir(path string) error {
	return os.RemoveAll(path)
}
