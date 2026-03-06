package hash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"sync"

	"deduplicatevideo/internal/geo"
	"deduplicatevideo/internal/model"
	"deduplicatevideo/internal/progress"
)

type hashJob struct {
	index int
	path  string
	info  fs.FileInfo
}

type hashResult struct {
	index int
	file  model.VideoFile
	err   error
}

func BuildVideoFiles(cfg model.Config, paths []string, infos map[string]fs.FileInfo) ([]model.VideoFile, []string) {
	jobs := make(chan hashJob)
	results := make(chan hashResult)
	errs := make([]string, 0)
	files := make([]model.VideoFile, len(paths))
	bar := progress.NewBar("视频摘要计算", len(paths))
	defer bar.Finish()

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				f, err := BuildSingleVideoFile(cfg, job.path, job.info)
				results <- hashResult{index: job.index, file: f, err: err}
			}
		}()
	}

	go func() {
		for i, p := range paths {
			jobs <- hashJob{index: i, path: p, info: infos[p]}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for r := range results {
		bar.Increment()
		if r.err != nil {
			errs = append(errs, fmt.Sprintf("摘要计算失败: %s: %v", paths[r.index], r.err))
			continue
		}
		files[r.index] = r.file
	}

	filtered := make([]model.VideoFile, 0, len(files))
	for _, f := range files {
		if f.Path != "" {
			filtered = append(filtered, f)
		}
	}
	return filtered, errs
}

func BuildSingleVideoFile(cfg model.Config, path string, info fs.FileInfo) (model.VideoFile, error) {
	sha, err := FileSHA256(path)
	if err != nil {
		return model.VideoFile{}, err
	}
	relDir, province, city := geo.ParseProvinceCity(cfg, path)
	samples, err := SampleChunkSHA(path, info.Size(), cfg.SampleChunkBytes)
	if err != nil {
		return model.VideoFile{}, err
	}
	return model.VideoFile{
		Path:        path,
		RelativeDir: relDir,
		Province:    province,
		City:        city,
		SizeBytes:   info.Size(),
		ModTime:     info.ModTime(),
		SHA256:      sha,
		SampleHash:  samples,
	}, nil
}

func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func SampleChunkSHA(path string, size, chunkSize int64) ([]string, error) {
	if size <= 0 {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	positions := []int64{0}
	if size > 1 {
		positions = append(positions, size/4, size/2, (size*3)/4)
		last := size - chunkSize
		if last < 0 {
			last = 0
		}
		positions = append(positions, last)
	}

	hashes := make([]string, 0, len(positions))
	seen := make(map[int64]struct{})
	buf := make([]byte, chunkSize)
	for _, pos := range positions {
		if _, ok := seen[pos]; ok {
			continue
		}
		seen[pos] = struct{}{}
		if _, err = f.Seek(pos, io.SeekStart); err != nil {
			return nil, err
		}
		n, readErr := io.ReadFull(f, buf)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return nil, readErr
		}
		if n <= 0 {
			continue
		}
		sum := sha256.Sum256(buf[:n])
		hashes = append(hashes, hex.EncodeToString(sum[:]))
	}
	sort.Strings(hashes)
	return hashes, nil
}
