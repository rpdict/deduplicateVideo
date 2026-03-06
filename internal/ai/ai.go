package ai

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"deduplicatevideo/internal/fsutil"
	"deduplicatevideo/internal/model"
	"deduplicatevideo/internal/progress"
)

func ExtractVideoFrames(cfg model.Config, files []model.VideoFile) (map[string]string, []string) {
	out := make(map[string]string, len(files))
	errs := make([]string, 0)
	bar := progress.NewBar("视频抽帧", len(files))
	defer bar.Finish()
	for _, f := range files {
		bar.Increment()
		base := strings.TrimSuffix(filepath.Base(f.Path), filepath.Ext(f.Path))
		framePath := filepath.Join(cfg.FrameOutputDir, base+".jpg")
		if err := ExtractSingleFrame(cfg, f.Path, framePath); err != nil {
			errs = append(errs, fmt.Sprintf("抽帧失败: %s: %v", f.Path, err))
			continue
		}
		out[f.Path] = framePath
	}
	return out, errs
}

func ExtractSingleFrame(cfg model.Config, videoPath, framePath string) error {
	if _, err := os.Stat(framePath); err == nil {
		return nil
	}
	if err := fsutil.EnsureDir(filepath.Dir(framePath)); err != nil {
		return err
	}
	cmd := []string{
		"ffmpeg",
		"-y",
		"-i", videoPath,
		"-vf", "scale=1280:720",
		"-frames:v", "1",
		framePath,
	}
	_, err := RunExternal(cmd)
	return err
}

func RecognizeEnvironments(cfg model.Config, frames map[string]string, prompt string) (map[string]model.AIResult, []string) {
	results := make(map[string]model.AIResult, len(frames))
	errs := make([]string, 0)
	bar := progress.NewBar("环境识别", len(frames))
	defer bar.Finish()
	for videoPath, framePath := range frames {
		bar.Increment()
		res, err := CallOllamaVision(cfg, prompt, framePath)
		if err != nil {
			errs = append(errs, fmt.Sprintf("识别失败: %s: %v", framePath, err))
			continue
		}
		results[videoPath] = res
	}
	return results, errs
}

func CallOllamaVision(cfg model.Config, prompt string, imagePath string) (model.AIResult, error) {
	imageBytes, err := os.ReadFile(imagePath)
	if err != nil {
		return model.AIResult{}, err
	}
	payload := map[string]interface{}{
		"model":  cfg.OllamaModel,
		"prompt": prompt,
		"stream": false,
		"images": []string{base64.StdEncoding.EncodeToString(imageBytes)},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return model.AIResult{}, err
	}
	url := strings.TrimSuffix(cfg.OllamaURL, "/") + "/api/generate"
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return model.AIResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return model.AIResult{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return model.AIResult{}, fmt.Errorf("ollama响应异常: %s", strings.TrimSpace(string(body)))
	}
	var raw struct {
		Response string `json:"response"`
	}
	if err = json.Unmarshal(body, &raw); err != nil {
		return model.AIResult{}, err
	}
	resText := strings.TrimSpace(raw.Response)
	var result model.AIResult
	if err = json.Unmarshal([]byte(resText), &result); err != nil {
		return model.AIResult{}, err
	}
	return result, nil
}

func RunExternal(cmd []string) (string, error) {
	if len(cmd) == 0 {
		return "", errors.New("空命令")
	}
	c := exec.Command(cmd[0], cmd[1:]...)
	out, err := c.CombinedOutput()
	return string(out), err
}
