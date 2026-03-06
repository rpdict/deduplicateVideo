package plan

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"deduplicatevideo/internal/ai"
	"deduplicatevideo/internal/fsutil"
	"deduplicatevideo/internal/model"
	"deduplicatevideo/internal/progress"
)

func BuildMovePlan(cfg model.Config, kept []model.VideoFile, dups []model.DuplicateRecord, otherPaths []string) (model.MovePlan, []string) {
	reserved := make(map[string]struct{})
	items := make([]model.MovePlanItem, 0, len(kept)+len(otherPaths)+len(dups))
	action := "copy"
	if !cfg.CopyMode {
		action = "move"
	}
	errs := make([]string, 0)

	promptText := ""
	if cfg.EnableAI {
		if data, err := os.ReadFile(cfg.PromptPath); err == nil {
			promptText = string(data)
		} else {
			errs = append(errs, fmt.Sprintf("读取提示词失败: %s: %v", cfg.PromptPath, err))
		}
	}
	if cfg.EnableAI && strings.TrimSpace(promptText) == "" {
		errs = append(errs, fmt.Sprintf("提示词内容为空: %s", cfg.PromptPath))
	}

	frames := make(map[string]string)
	aiResults := make(map[string]model.AIResult)
	if cfg.EnableAI {
		if err := fsutil.EnsureDir(cfg.FrameOutputDir); err == nil {
			var frameErrs []string
			frames, frameErrs = ai.ExtractVideoFrames(cfg, kept)
			errs = append(errs, frameErrs...)
			if len(frames) == 0 && len(kept) > 0 {
				errs = append(errs, "未生成任何视频帧，AI识别未执行")
			}
			var aiErrs []string
			aiResults, aiErrs = ai.RecognizeEnvironments(cfg, frames, promptText)
			errs = append(errs, aiErrs...)
		} else {
			errs = append(errs, fmt.Sprintf("创建抽帧目录失败: %s: %v", cfg.FrameOutputDir, err))
		}
	}

	for _, f := range kept {
		destDir := TargetVideoDir(cfg, f)
		if cfg.EnableAI {
			if env, ok := aiResults[f.Path]; ok && env.Environment != "" && env.Environment != "unknown" && env.Confidence >= cfg.EnvMinConfidence {
				destDir = filepath.Join(destDir, env.Environment)
			}
		}
		destPath, err := fsutil.PlanUniqueDestPath(reserved, destDir, filepath.Base(f.Path))
		if err != nil {
			continue
		}
		framePath := frames[f.Path]
		env := aiResults[f.Path]
		items = append(items, model.MovePlanItem{
			SourcePath:  f.Path,
			DestPath:    destPath,
			Action:      action,
			Approved:    false,
			Reason:      "keep_video",
			Environment: env.Environment,
			Confidence:  env.Confidence,
			FramePath:   framePath,
		})
	}

	if cfg.KeepDuplicateCopies {
		dupDir := filepath.Join(cfg.OutputDir, "_duplicates")
		for _, d := range dups {
			destPath, err := fsutil.PlanUniqueDestPath(reserved, dupDir, filepath.Base(d.OriginalPath))
			if err != nil {
				continue
			}
			items = append(items, model.MovePlanItem{
				SourcePath: d.OriginalPath,
				DestPath:   destPath,
				Action:     "copy",
				Approved:   false,
				Reason:     "duplicate_copy",
			})
		}
	}

	for _, src := range otherPaths {
		rel, err := filepath.Rel(cfg.InputDir, src)
		if err != nil {
			continue
		}
		destPath := filepath.Join(cfg.OutputDir, rel)
		destDir := filepath.Dir(destPath)
		destPath, err = fsutil.PlanUniqueDestPath(reserved, destDir, filepath.Base(destPath))
		if err != nil {
			continue
		}
		items = append(items, model.MovePlanItem{
			SourcePath: src,
			DestPath:   destPath,
			Action:     action,
			Approved:   false,
			Reason:     "non_video",
		})
	}

	return model.MovePlan{
		GeneratedAt: time.Now().Format(time.RFC3339),
		InputDir:    cfg.InputDir,
		OutputDir:   cfg.OutputDir,
		Items:       items,
	}, errs
}

func WriteMovePlanJSON(path string, plan model.MovePlan) error {
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func WriteMovePlanCSV(path string, items []model.MovePlanItem) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err = w.Write([]string{"source_path", "dest_path", "action", "approved", "reason", "environment", "confidence", "frame_path"}); err != nil {
		return err
	}
	for _, item := range items {
		row := []string{
			item.SourcePath,
			item.DestPath,
			item.Action,
			fmt.Sprintf("%t", item.Approved),
			item.Reason,
			item.Environment,
			fmt.Sprintf("%.4f", item.Confidence),
			item.FramePath,
		}
		if err = w.Write(row); err != nil {
			return err
		}
	}
	return w.Error()
}

func ReadMovePlan(path string) (model.MovePlan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.MovePlan{}, err
	}
	var plan model.MovePlan
	if err = json.Unmarshal(data, &plan); err != nil {
		return model.MovePlan{}, err
	}
	return plan, nil
}

func ExecuteMovePlan(cfg model.Config, items []model.MovePlanItem) []string {
	errs := make([]string, 0)
	bar := progress.NewBar("文件移动", len(items))
	defer bar.Finish()
	for _, item := range items {
		bar.Increment()
		if !item.Approved {
			continue
		}
		destDir := filepath.Dir(item.DestPath)
		if err := fsutil.EnsureDir(destDir); err != nil {
			errs = append(errs, fmt.Sprintf("创建目录失败: %s: %v", destDir, err))
			continue
		}
		var err error
		if item.Action == "copy" {
			err = fsutil.CopyFile(item.SourcePath, item.DestPath)
		} else {
			err = fsutil.MoveFile(item.SourcePath, item.DestPath)
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("执行失败: %s -> %s: %v", item.SourcePath, item.DestPath, err))
		}
	}
	return errs
}

func ConfirmMove() bool {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("\n已生成 move_plan.json 与 move_plan.csv，是否继续移动文件？(yes/no): ")
		text, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return false
		}
		text = strings.TrimSpace(strings.ToLower(text))
		if text == "y" || text == "yes" {
			return true
		}
		if text == "" || text == "n" || text == "no" {
			return false
		}
	}
}

func TargetVideoDir(cfg model.Config, f model.VideoFile) string {
	if ShouldFallbackToOriginalDir(cfg, f) {
		if f.RelativeDir == "" || f.RelativeDir == "." {
			return cfg.OutputDir
		}
		return filepath.Join(cfg.OutputDir, f.RelativeDir)
	}
	return filepath.Join(cfg.OutputDir, f.Province, f.City)
}

func ShouldFallbackToOriginalDir(cfg model.Config, f model.VideoFile) bool {
	province := strings.TrimSpace(f.Province)
	city := strings.TrimSpace(f.City)
	if province == "" || city == "" {
		return true
	}
	return province == cfg.UnknownProvince || city == cfg.UnknownCity
}

func OrganizeNonVideoFiles(cfg model.Config, paths []string) ([]model.OrganizeRecord, []string) {
	records := make([]model.OrganizeRecord, 0, len(paths))
	errs := make([]string, 0)
	bar := progress.NewBar("非视频归档", len(paths))
	defer bar.Finish()
	for _, src := range paths {
		bar.Increment()
		rel, err := filepath.Rel(cfg.InputDir, src)
		if err != nil {
			errs = append(errs, fmt.Sprintf("计算相对路径失败: %s: %v", src, err))
			continue
		}
		destPath := filepath.Join(cfg.OutputDir, rel)
		destDir := filepath.Dir(destPath)
		if err = fsutil.EnsureDir(destDir); err != nil {
			errs = append(errs, fmt.Sprintf("创建目录失败: %s: %v", destDir, err))
			continue
		}
		if _, statErr := os.Stat(destPath); statErr == nil {
			destPath, err = fsutil.UniqueDestPath(destDir, filepath.Base(src))
			if err != nil {
				errs = append(errs, fmt.Sprintf("生成目标路径失败: %s: %v", src, err))
				continue
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			errs = append(errs, fmt.Sprintf("检查目标路径失败: %s: %v", destPath, statErr))
			continue
		}
		if err = fsutil.TransferFile(src, destPath, cfg.CopyMode); err != nil {
			errs = append(errs, fmt.Sprintf("归档非视频文件失败: %s -> %s: %v", src, destPath, err))
			continue
		}
		records = append(records, model.OrganizeRecord{
			SourcePath: src,
			DestPath:   destPath,
		})
	}
	return records, errs
}
