package scan

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

var videoExtSet = map[string]struct{}{
	".mp4":  {},
	".mov":  {},
	".mkv":  {},
	".avi":  {},
	".flv":  {},
	".wmv":  {},
	".m4v":  {},
	".webm": {},
}

func ScanVideoFiles(root string) ([]string, map[string]fs.FileInfo, []string) {
	paths := make([]string, 0)
	infos := make(map[string]fs.FileInfo)
	errs := make([]string, 0)
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			errs = append(errs, fmt.Sprintf("遍历失败: %s: %v", path, err))
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !IsVideoFile(d.Name()) {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			errs = append(errs, fmt.Sprintf("读取文件信息失败: %s: %v", path, infoErr))
			return nil
		}
		paths = append(paths, path)
		infos[path] = info
		return nil
	})
	sort.Strings(paths)
	return paths, infos, errs
}

func ScanNonVideoFiles(root string) ([]string, []string) {
	paths := make([]string, 0)
	errs := make([]string, 0)
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			errs = append(errs, fmt.Sprintf("遍历失败: %s: %v", path, err))
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if IsVideoFile(d.Name()) {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	sort.Strings(paths)
	return paths, errs
}

func IsVideoFile(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	_, ok := videoExtSet[ext]
	return ok
}
