package fsutil

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func EnsureDir(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

func UniqueDestPath(dir, fileName string) (string, error) {
	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	candidate := filepath.Join(dir, fileName)
	if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
		return candidate, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	for i := 1; i <= 100000; i++ {
		name := fmt.Sprintf("%s_%d%s", base, i, ext)
		candidate = filepath.Join(dir, name)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		}
	}
	return "", errors.New("文件名冲突次数过多")
}

func TransferFile(src, dst string, copyMode bool) error {
	if copyMode {
		return CopyFile(src, dst)
	}
	return MoveFile(src, dst)
}

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func MoveFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if err := CopyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

func PlanUniqueDestPath(reserved map[string]struct{}, dir, fileName string) (string, error) {
	if err := EnsureDir(dir); err != nil {
		return "", err
	}
	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	candidate := filepath.Join(dir, fileName)
	if !IsPathTaken(candidate, reserved) {
		reserved[candidate] = struct{}{}
		return candidate, nil
	}
	for i := 1; i <= 100000; i++ {
		name := fmt.Sprintf("%s_%d%s", base, i, ext)
		candidate = filepath.Join(dir, name)
		if !IsPathTaken(candidate, reserved) {
			reserved[candidate] = struct{}{}
			return candidate, nil
		}
	}
	return "", errors.New("文件名冲突次数过多")
}

func IsPathTaken(path string, reserved map[string]struct{}) bool {
	if _, ok := reserved[path]; ok {
		return true
	}
	if _, err := os.Stat(path); err == nil {
		return true
	} else if !errors.Is(err, os.ErrNotExist) {
		return true
	}
	return false
}
