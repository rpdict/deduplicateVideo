package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"deduplicatevideo/internal/model"
)

func ParseFlags() (model.Config, error) {
	var cfg model.Config
	flag.StringVar(&cfg.InputDir, "inputDir", "", "待处理视频根目录")
	flag.StringVar(&cfg.OutputDir, "outputDir", "", "归类输出目录")
	flag.IntVar(&cfg.WorkerCount, "workers", 4, "并发数量")
	flag.Float64Var(&cfg.NearDupThreshold, "nearThreshold", 0.8, "近重复阈值(0-1)")
	flag.Float64Var(&cfg.MaxSizeDiffRatio, "maxSizeDiffRatio", 0.2, "近重复大小差异比例阈值")
	flag.Int64Var(&cfg.SampleChunkBytes, "sampleChunkBytes", 65536, "抽样块字节数")
	flag.BoolVar(&cfg.KeepDuplicateCopies, "keepDuplicateCopies", false, "是否保留重复副本到输出")
	flag.BoolVar(&cfg.CopyMode, "copyMode", true, "true复制,false移动")
	flag.StringVar(&cfg.Step, "step", "plan", "执行步骤: plan 或 move")
	flag.StringVar(&cfg.PlanJSONPath, "planJson", "", "规划文件JSON路径")
	flag.StringVar(&cfg.PlanCSVPath, "planCsv", "", "规划文件CSV路径")
	flag.BoolVar(&cfg.ConfirmMove, "confirmMove", true, "计划生成后是否等待确认再移动")
	flag.BoolVar(&cfg.EnableAI, "enableAI", false, "是否启用AI识别环境")
	flag.StringVar(&cfg.FrameOutputDir, "frameDir", "", "抽帧输出目录")
	flag.StringVar(&cfg.PromptPath, "promptPath", "", "AI识别提示词路径")
	flag.Float64Var(&cfg.EnvMinConfidence, "envMinConfidence", 0.6, "环境识别置信度阈值")
	flag.StringVar(&cfg.UnknownProvince, "unknownProvince", "待分类", "未知省份目录名")
	flag.StringVar(&cfg.UnknownCity, "unknownCity", "未知城市", "未知城市目录名")
	flag.StringVar(&cfg.OllamaURL, "ollamaURL", "http://localhost:11434/", "本地Ollama地址")
	flag.StringVar(&cfg.OllamaModel, "ollamaModel", "qwen3-vl:8b", "模型名称")
	flag.Parse()

	if cfg.InputDir == "" || cfg.OutputDir == "" {
		return cfg, errors.New("inputDir 与 outputDir 为必填")
	}
	if cfg.WorkerCount <= 0 {
		return cfg, errors.New("workers 必须大于0")
	}
	if cfg.SampleChunkBytes <= 0 {
		return cfg, errors.New("sampleChunkBytes 必须大于0")
	}
	if cfg.NearDupThreshold < 0 || cfg.NearDupThreshold > 1 {
		return cfg, errors.New("nearThreshold 必须在0到1之间")
	}
	if cfg.MaxSizeDiffRatio < 0 || cfg.MaxSizeDiffRatio > 1 {
		return cfg, errors.New("maxSizeDiffRatio 必须在0到1之间")
	}
	if cfg.EnvMinConfidence < 0 || cfg.EnvMinConfidence > 1 {
		return cfg, errors.New("envMinConfidence 必须在0到1之间")
	}
	if cfg.Step != "plan" && cfg.Step != "move" {
		return cfg, errors.New("step 必须为 plan 或 move")
	}

	absInput, err := filepath.Abs(cfg.InputDir)
	if err != nil {
		return cfg, err
	}
	absOutput, err := filepath.Abs(cfg.OutputDir)
	if err != nil {
		return cfg, err
	}
	cfg.InputDir = absInput
	cfg.OutputDir = absOutput
	if cfg.PlanJSONPath == "" {
		cfg.PlanJSONPath = filepath.Join(cfg.OutputDir, "move_plan.json")
	}
	if cfg.PlanCSVPath == "" {
		cfg.PlanCSVPath = filepath.Join(cfg.OutputDir, "move_plan.csv")
	}
	if cfg.FrameOutputDir == "" {
		cfg.FrameOutputDir = filepath.Join(cfg.OutputDir, "_frames")
	}
	if cfg.PromptPath == "" {
		if cwd, err := os.Getwd(); err == nil {
			cfg.PromptPath = filepath.Join(cwd, "configs", "prompts", "video_frame_recognition_prompt.md")
		}
	}
	if cfg.EnableAI {
		if _, err := os.Stat(cfg.PromptPath); err != nil {
			return cfg, fmt.Errorf("promptPath 不存在: %s", cfg.PromptPath)
		}
		if _, err := exec.LookPath("ffmpeg"); err != nil {
			return cfg, errors.New("未找到 ffmpeg，可通过安装 ffmpeg 并加入 PATH 后再启用AI")
		}
	}
	return cfg, nil
}
