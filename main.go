package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
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

// Config 定义命令行可配置项。
type Config struct {
	InputDir            string
	OutputDir           string
	WorkerCount         int
	NearDupThreshold    float64
	MaxSizeDiffRatio    float64
	SampleChunkBytes    int64
	KeepDuplicateCopies bool
	CopyMode            bool
	Step                string
	PlanJSONPath        string
	PlanCSVPath         string
	ConfirmMove         bool
	EnableAI            bool
	FrameOutputDir      string
	PromptPath          string
	EnvMinConfidence    float64
	UnknownProvince     string
	UnknownCity         string
	OllamaURL           string
	OllamaModel         string
}

// VideoFile 表示单个视频在处理流程中的元信息与摘要信息。
type VideoFile struct {
	Path        string
	RelativeDir string
	Province    string
	City        string
	SizeBytes   int64
	ModTime     time.Time
	SHA256      string
	SampleHash  []string
}

// DuplicateRecord 记录被判定为重复的视频及保留关系。
type DuplicateRecord struct {
	OriginalPath string  `json:"original_path"`
	KeepPath     string  `json:"keep_path"`
	Reason       string  `json:"reason"`
	Similarity   float64 `json:"similarity"`
}

// OrganizeRecord 记录文件归档前后的路径映射关系。
type OrganizeRecord struct {
	SourcePath string `json:"source_path"`
	DestPath   string `json:"dest_path"`
	Province   string `json:"province"`
	City       string `json:"city"`
}

// Report 汇总整次任务的统计结果与明细。
type Report struct {
	GeneratedAt      string            `json:"generated_at"`
	InputDir         string            `json:"input_dir"`
	OutputDir        string            `json:"output_dir"`
	TotalScanned     int               `json:"total_scanned"`
	TotalUniqueKept  int               `json:"total_unique_kept"`
	TotalDuplicates  int               `json:"total_duplicates"`
	Errors           []string          `json:"errors"`
	DuplicateRecords []DuplicateRecord `json:"duplicate_records"`
	OrganizedRecords []OrganizeRecord  `json:"organized_records"`
}

type MovePlan struct {
	GeneratedAt string         `json:"generated_at"`
	InputDir    string         `json:"input_dir"`
	OutputDir   string         `json:"output_dir"`
	Items       []MovePlanItem `json:"items"`
}

type MovePlanItem struct {
	SourcePath string `json:"source_path"`
	DestPath   string `json:"dest_path"`
	Action     string `json:"action"`
	Approved   bool   `json:"approved"`
	Reason     string `json:"reason"`
	Environment string `json:"environment"`
	Confidence  float64 `json:"confidence"`
	FramePath   string `json:"frame_path"`
}

type hashJob struct {
	index int
	path  string
	info  fs.FileInfo
}

type hashResult struct {
	index int
	file  VideoFile
	err   error
}

type progressBar struct {
	label   string
	total   int
	current int
	width   int
}

type disjointSet struct {
	parent []int
	rank   []int
}

// newDisjointSet 创建并查集，用于近重复聚类。
func newDisjointSet(n int) *disjointSet {
	parent := make([]int, n)
	rank := make([]int, n)
	for i := 0; i < n; i++ {
		parent[i] = i
	}
	return &disjointSet{parent: parent, rank: rank}
}

// find 查找元素所属集合的根节点（含路径压缩）。
func (d *disjointSet) find(x int) int {
	if d.parent[x] != x {
		d.parent[x] = d.find(d.parent[x])
	}
	return d.parent[x]
}

// union 合并两个集合。
func (d *disjointSet) union(a, b int) {
	ra := d.find(a)
	rb := d.find(b)
	if ra == rb {
		return
	}
	if d.rank[ra] < d.rank[rb] {
		d.parent[ra] = rb
		return
	}
	if d.rank[ra] > d.rank[rb] {
		d.parent[rb] = ra
		return
	}
	d.parent[rb] = ra
	d.rank[ra]++
}

func newProgressBar(label string, total int) *progressBar {
	if total <= 0 {
		return nil
	}
	p := &progressBar{
		label: label,
		total: total,
		width: 32,
	}
	p.render(false)
	return p
}

func (p *progressBar) increment() {
	if p == nil {
		return
	}
	if p.current < p.total {
		p.current++
	}
	p.render(false)
}

func (p *progressBar) finish() {
	if p == nil {
		return
	}
	p.current = p.total
	p.render(true)
}

func (p *progressBar) render(final bool) {
	if p == nil || p.total <= 0 {
		return
	}
	if p.current > p.total {
		p.current = p.total
	}
	ratio := float64(p.current) / float64(p.total)
	filled := int(ratio * float64(p.width))
	if filled > p.width {
		filled = p.width
	}
	bar := strings.Repeat("=", filled) + strings.Repeat(".", p.width-filled)
	percent := ratio * 100
	fmt.Printf("\r%s [%s] %6.2f%% (%d/%d)", p.label, bar, percent, p.current, p.total)
	if final {
		fmt.Print("\n")
	}
}

// main 作为程序入口，执行参数解析、主流程运行与报告写入。
func main() {
	ensureUTF8Console()
	cfg, err := parseFlags()
	if err != nil {
		log.Fatalf("参数错误: %v", err)
	}
	if err = ensureDir(cfg.OutputDir); err != nil {
		log.Fatalf("无法创建输出目录: %v", err)
	}

	start := time.Now()
	report, err := run(cfg)
	if err != nil {
		log.Fatalf("执行失败: %v", err)
	}

	if err = writeReports(cfg.OutputDir, report); err != nil {
		log.Fatalf("写入报告失败: %v", err)
	}

	log.Printf("完成: 扫描=%d, 保留=%d, 重复=%d, 耗时=%s",
		report.TotalScanned, report.TotalUniqueKept, report.TotalDuplicates, time.Since(start).String())
}

func ensureUTF8Console() {
	if runtime.GOOS != "windows" {
		return
	}
	cmd := exec.Command("cmd", "/c", "chcp", "65001")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	_ = cmd.Run()
}

// parseFlags 读取并校验命令行参数。
func parseFlags() (Config, error) {
	var cfg Config
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

// run 串联扫描、摘要、去重、归类与报告数据组装。
func run(cfg Config) (Report, error) {
	if cfg.Step == "move" {
		plan, err := readMovePlan(cfg.PlanJSONPath)
		if err != nil {
			return Report{}, err
		}
		errs := executeMovePlan(cfg, plan.Items)
		report := Report{
			GeneratedAt: time.Now().Format(time.RFC3339),
			InputDir:    cfg.InputDir,
			OutputDir:   cfg.OutputDir,
		}
		report.Errors = append(report.Errors, errs...)
		return report, nil
	}

	paths, infos, walkErrs := scanVideoFiles(cfg.InputDir)
	otherPaths, otherScanErrs := scanNonVideoFiles(cfg.InputDir)
	files, hashErrs := buildVideoFiles(cfg, paths, infos)

	report := Report{
		GeneratedAt: time.Now().Format(time.RFC3339),
		InputDir:    cfg.InputDir,
		OutputDir:   cfg.OutputDir,
		TotalScanned: len(paths) + len(otherPaths),
	}
	report.Errors = append(report.Errors, walkErrs...)
	report.Errors = append(report.Errors, otherScanErrs...)
	report.Errors = append(report.Errors, hashErrs...)

	kept, dupRecords := deduplicate(files, cfg.NearDupThreshold, cfg.MaxSizeDiffRatio)
	report.DuplicateRecords = dupRecords
	report.TotalDuplicates = len(dupRecords)
	report.TotalUniqueKept = len(kept)

	plan, planErrs := buildMovePlan(cfg, kept, dupRecords, otherPaths)
	report.Errors = append(report.Errors, planErrs...)
	if err := writeMovePlanJSON(cfg.PlanJSONPath, plan); err != nil {
		report.Errors = append(report.Errors, err.Error())
	}
	if err := writeMovePlanCSV(cfg.PlanCSVPath, plan.Items); err != nil {
		report.Errors = append(report.Errors, err.Error())
	}

	if cfg.ConfirmMove {
		if confirmMove() {
			for i := range plan.Items {
				plan.Items[i].Approved = true
			}
			_ = writeMovePlanJSON(cfg.PlanJSONPath, plan)
			_ = writeMovePlanCSV(cfg.PlanCSVPath, plan.Items)
			errs := executeMovePlan(cfg, plan.Items)
			report.Errors = append(report.Errors, errs...)
		}
	}

	return report, nil
}

// scanVideoFiles 递归扫描输入目录，返回符合扩展名的视频文件路径及文件信息。
func scanVideoFiles(root string) ([]string, map[string]fs.FileInfo, []string) {
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
		if !isVideoFile(d.Name()) {
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

// scanNonVideoFiles 递归扫描输入目录，收集非视频文件路径。
func scanNonVideoFiles(root string) ([]string, []string) {
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
		if isVideoFile(d.Name()) {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	sort.Strings(paths)
	return paths, errs
}

// isVideoFile 判断文件名是否属于视频扩展名。
func isVideoFile(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	_, ok := videoExtSet[ext]
	return ok
}

func buildMovePlan(cfg Config, kept []VideoFile, dups []DuplicateRecord, otherPaths []string) (MovePlan, []string) {
	reserved := make(map[string]struct{})
	items := make([]MovePlanItem, 0, len(kept)+len(otherPaths)+len(dups))
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
	aiResults := make(map[string]AIResult)
	if cfg.EnableAI {
		if err := ensureDir(cfg.FrameOutputDir); err == nil {
			var frameErrs []string
			frames, frameErrs = extractVideoFrames(cfg, kept)
			errs = append(errs, frameErrs...)
			if len(frames) == 0 && len(kept) > 0 {
				errs = append(errs, "未生成任何视频帧，AI识别未执行")
			}
			var aiErrs []string
			aiResults, aiErrs = recognizeEnvironments(cfg, frames, promptText)
			errs = append(errs, aiErrs...)
		} else {
			errs = append(errs, fmt.Sprintf("创建抽帧目录失败: %s: %v", cfg.FrameOutputDir, err))
		}
	}

	for _, f := range kept {
		destDir := targetVideoDir(cfg, f)
		if cfg.EnableAI {
			if env, ok := aiResults[f.Path]; ok && env.Environment != "" && env.Environment != "unknown" && env.Confidence >= cfg.EnvMinConfidence {
				destDir = filepath.Join(destDir, env.Environment)
			}
		}
		destPath, err := planUniqueDestPath(reserved, destDir, filepath.Base(f.Path))
		if err != nil {
			continue
		}
		framePath := frames[f.Path]
		env := aiResults[f.Path]
		items = append(items, MovePlanItem{
			SourcePath: f.Path,
			DestPath:   destPath,
			Action:     action,
			Approved:   false,
			Reason:     "keep_video",
			Environment: env.Environment,
			Confidence:  env.Confidence,
			FramePath:   framePath,
		})
	}

	if cfg.KeepDuplicateCopies {
		dupDir := filepath.Join(cfg.OutputDir, "_duplicates")
		for _, d := range dups {
			destPath, err := planUniqueDestPath(reserved, dupDir, filepath.Base(d.OriginalPath))
			if err != nil {
				continue
			}
			items = append(items, MovePlanItem{
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
		destPath, err = planUniqueDestPath(reserved, destDir, filepath.Base(destPath))
		if err != nil {
			continue
		}
		items = append(items, MovePlanItem{
			SourcePath: src,
			DestPath:   destPath,
			Action:     action,
			Approved:   false,
			Reason:     "non_video",
		})
	}

	return MovePlan{
		GeneratedAt: time.Now().Format(time.RFC3339),
		InputDir:    cfg.InputDir,
		OutputDir:   cfg.OutputDir,
		Items:       items,
	}, errs
}

type AIResult struct {
	Environment string  `json:"environment"`
	Confidence  float64 `json:"confidence"`
	Summary     string  `json:"summary"`
	Tags        []string `json:"tags"`
}

func extractVideoFrames(cfg Config, files []VideoFile) (map[string]string, []string) {
	out := make(map[string]string, len(files))
	errs := make([]string, 0)
	bar := newProgressBar("视频抽帧", len(files))
	defer bar.finish()
	for _, f := range files {
		bar.increment()
		base := strings.TrimSuffix(filepath.Base(f.Path), filepath.Ext(f.Path))
		framePath := filepath.Join(cfg.FrameOutputDir, base+".jpg")
		if err := extractSingleFrame(cfg, f.Path, framePath); err != nil {
			errs = append(errs, fmt.Sprintf("抽帧失败: %s: %v", f.Path, err))
			continue
		}
		out[f.Path] = framePath
	}
	return out, errs
}

func extractSingleFrame(cfg Config, videoPath, framePath string) error {
	if _, err := os.Stat(framePath); err == nil {
		return nil
	}
	if err := ensureDir(filepath.Dir(framePath)); err != nil {
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
	_, err := runExternal(cmd)
	return err
}

func recognizeEnvironments(cfg Config, frames map[string]string, prompt string) (map[string]AIResult, []string) {
	results := make(map[string]AIResult, len(frames))
	errs := make([]string, 0)
	bar := newProgressBar("环境识别", len(frames))
	defer bar.finish()
	for videoPath, framePath := range frames {
		bar.increment()
		res, err := callOllamaVision(cfg, prompt, framePath)
		if err != nil {
			errs = append(errs, fmt.Sprintf("识别失败: %s: %v", framePath, err))
			continue
		}
		results[videoPath] = res
	}
	return results, errs
}

func callOllamaVision(cfg Config, prompt string, imagePath string) (AIResult, error) {
	imageBytes, err := os.ReadFile(imagePath)
	if err != nil {
		return AIResult{}, err
	}
	payload := map[string]interface{}{
		"model":  cfg.OllamaModel,
		"prompt": prompt,
		"stream": false,
		"images": []string{base64.StdEncoding.EncodeToString(imageBytes)},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return AIResult{}, err
	}
	url := strings.TrimSuffix(cfg.OllamaURL, "/") + "/api/generate"
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return AIResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return AIResult{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return AIResult{}, fmt.Errorf("ollama响应异常: %s", strings.TrimSpace(string(body)))
	}
	var raw struct {
		Response string `json:"response"`
	}
	if err = json.Unmarshal(body, &raw); err != nil {
		return AIResult{}, err
	}
	resText := strings.TrimSpace(raw.Response)
	var result AIResult
	if err = json.Unmarshal([]byte(resText), &result); err != nil {
		return AIResult{}, err
	}
	return result, nil
}

func runExternal(cmd []string) (string, error) {
	if len(cmd) == 0 {
		return "", errors.New("空命令")
	}
	c := exec.Command(cmd[0], cmd[1:]...)
	out, err := c.CombinedOutput()
	return string(out), err
}

func planUniqueDestPath(reserved map[string]struct{}, dir, fileName string) (string, error) {
	if err := ensureDir(dir); err != nil {
		return "", err
	}
	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	candidate := filepath.Join(dir, fileName)
	if !isPathTaken(candidate, reserved) {
		reserved[candidate] = struct{}{}
		return candidate, nil
	}
	for i := 1; i <= 100000; i++ {
		name := fmt.Sprintf("%s_%d%s", base, i, ext)
		candidate = filepath.Join(dir, name)
		if !isPathTaken(candidate, reserved) {
			reserved[candidate] = struct{}{}
			return candidate, nil
		}
	}
	return "", errors.New("文件名冲突次数过多")
}

func isPathTaken(path string, reserved map[string]struct{}) bool {
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

func writeMovePlanJSON(path string, plan MovePlan) error {
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func writeMovePlanCSV(path string, items []MovePlanItem) error {
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

func readMovePlan(path string) (MovePlan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return MovePlan{}, err
	}
	var plan MovePlan
	if err = json.Unmarshal(data, &plan); err != nil {
		return MovePlan{}, err
	}
	return plan, nil
}

func executeMovePlan(cfg Config, items []MovePlanItem) []string {
	errs := make([]string, 0)
	bar := newProgressBar("文件移动", len(items))
	defer bar.finish()
	for _, item := range items {
		bar.increment()
		if !item.Approved {
			continue
		}
		destDir := filepath.Dir(item.DestPath)
		if err := ensureDir(destDir); err != nil {
			errs = append(errs, fmt.Sprintf("创建目录失败: %s: %v", destDir, err))
			continue
		}
		var err error
		if item.Action == "copy" {
			err = copyFile(item.SourcePath, item.DestPath)
		} else {
			err = moveFile(item.SourcePath, item.DestPath)
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("执行失败: %s -> %s: %v", item.SourcePath, item.DestPath, err))
		}
	}
	return errs
}

func confirmMove() bool {
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

// buildVideoFiles 并发计算视频摘要与路径归类信息。
func buildVideoFiles(cfg Config, paths []string, infos map[string]fs.FileInfo) ([]VideoFile, []string) {
	jobs := make(chan hashJob)
	results := make(chan hashResult)
	errs := make([]string, 0)
	files := make([]VideoFile, len(paths))
	bar := newProgressBar("视频摘要计算", len(paths))
	defer bar.finish()

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				f, err := buildSingleVideoFile(cfg, job.path, job.info)
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
		bar.increment()
		if r.err != nil {
			errs = append(errs, fmt.Sprintf("摘要计算失败: %s: %v", paths[r.index], r.err))
			continue
		}
		files[r.index] = r.file
	}

	filtered := make([]VideoFile, 0, len(files))
	for _, f := range files {
		if f.Path != "" {
			filtered = append(filtered, f)
		}
	}
	return filtered, errs
}

// buildSingleVideoFile 计算单个文件的强哈希与抽样哈希，并解析省市信息。
func buildSingleVideoFile(cfg Config, path string, info fs.FileInfo) (VideoFile, error) {
	sha, err := fileSHA256(path)
	if err != nil {
		return VideoFile{}, err
	}
	relDir, province, city := parseProvinceCity(cfg, path)
	samples, err := sampleChunkSHA(path, info.Size(), cfg.SampleChunkBytes)
	if err != nil {
		return VideoFile{}, err
	}
	return VideoFile{
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

// parseProvinceCity 从 inputDir 之后的路径层级提取省份与城市。
func parseProvinceCity(cfg Config, absPath string) (string, string, string) {
	rel, err := filepath.Rel(cfg.InputDir, absPath)
	if err != nil {
		return "", cfg.UnknownProvince, cfg.UnknownCity
	}
	dir := filepath.Dir(rel)
	if dir == "." || dir == "" {
		return "", cfg.UnknownProvince, cfg.UnknownCity
	}
	parts := strings.Split(filepath.ToSlash(dir), "/")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" && p != "." {
			clean = append(clean, p)
		}
	}
	if len(clean) == 0 {
		return dir, cfg.UnknownProvince, cfg.UnknownCity
	}
	if len(clean) == 1 {
		return dir, normalizeGeo(clean[0], cfg.UnknownProvince), cfg.UnknownCity
	}
	return dir, normalizeGeo(clean[0], cfg.UnknownProvince), normalizeGeo(clean[1], cfg.UnknownCity)
}

// normalizeGeo 归一化地理名称并过滤非法路径字符。
func normalizeGeo(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	v = strings.ReplaceAll(v, "\\", "_")
	v = strings.ReplaceAll(v, "/", "_")
	return v
}

// fileSHA256 计算文件全量 SHA-256 摘要，用于精确去重。
func fileSHA256(path string) (string, error) {
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

// sampleChunkSHA 计算多位置抽样块哈希，用于近重复候选比对。
func sampleChunkSHA(path string, size, chunkSize int64) ([]string, error) {
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

// deduplicate 先按全量哈希精确去重，再按抽样相似度进行近重复聚类。
func deduplicate(files []VideoFile, nearThreshold, maxSizeDiffRatio float64) ([]VideoFile, []DuplicateRecord) {
	if len(files) == 0 {
		return nil, nil
	}
	byHash := make(map[string][]int)
	for i := range files {
		byHash[files[i].SHA256] = append(byHash[files[i].SHA256], i)
	}

	keptFlag := make([]bool, len(files))
	dupRecords := make([]DuplicateRecord, 0)
	uniqueCandidates := make([]int, 0)

	for _, idxs := range byHash {
		if len(idxs) == 1 {
			keptFlag[idxs[0]] = true
			uniqueCandidates = append(uniqueCandidates, idxs[0])
			continue
		}
		keep := pickBest(files, idxs)
		keptFlag[keep] = true
		uniqueCandidates = append(uniqueCandidates, keep)
		for _, idx := range idxs {
			if idx == keep {
				continue
			}
			dupRecords = append(dupRecords, DuplicateRecord{
				OriginalPath: files[idx].Path,
				KeepPath:     files[keep].Path,
				Reason:       "exact_hash",
				Similarity:   1.0,
			})
		}
	}

	ds := newDisjointSet(len(uniqueCandidates))
	totalPairs := len(uniqueCandidates) * (len(uniqueCandidates) - 1) / 2
	compareBar := newProgressBar("近重复比对", totalPairs)
	defer compareBar.finish()
	for i := 0; i < len(uniqueCandidates); i++ {
		for j := i + 1; j < len(uniqueCandidates); j++ {
			compareBar.increment()
			a := files[uniqueCandidates[i]]
			b := files[uniqueCandidates[j]]
			if !withinSizeDiff(a.SizeBytes, b.SizeBytes, maxSizeDiffRatio) {
				continue
			}
			sim := sampleSimilarity(a.SampleHash, b.SampleHash)
			if sim >= nearThreshold {
				ds.union(i, j)
			}
		}
	}

	clusterMap := make(map[int][]int)
	for i := range uniqueCandidates {
		root := ds.find(i)
		clusterMap[root] = append(clusterMap[root], uniqueCandidates[i])
	}

	finalKept := make([]VideoFile, 0, len(clusterMap))
	for _, idxs := range clusterMap {
		if len(idxs) == 0 {
			continue
		}
		keep := pickBest(files, idxs)
		finalKept = append(finalKept, files[keep])
		for _, idx := range idxs {
			if idx == keep {
				continue
			}
			dupRecords = append(dupRecords, DuplicateRecord{
				OriginalPath: files[idx].Path,
				KeepPath:     files[keep].Path,
				Reason:       "near_duplicate",
				Similarity:   sampleSimilarity(files[idx].SampleHash, files[keep].SampleHash),
			})
		}
	}

	sort.Slice(finalKept, func(i, j int) bool {
		return finalKept[i].Path < finalKept[j].Path
	})
	sort.Slice(dupRecords, func(i, j int) bool {
		return dupRecords[i].OriginalPath < dupRecords[j].OriginalPath
	})
	return finalKept, dupRecords
}

// withinSizeDiff 判断两个文件大小是否在可比较范围内。
func withinSizeDiff(a, b int64, maxRatio float64) bool {
	if a <= 0 || b <= 0 {
		return false
	}
	maxV := float64(a)
	minV := float64(b)
	if minV > maxV {
		maxV, minV = minV, maxV
	}
	diffRatio := (maxV - minV) / maxV
	return diffRatio <= maxRatio
}

// sampleSimilarity 计算两组抽样哈希的 Jaccard 相似度。
func sampleSimilarity(a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	setA := make(map[string]struct{}, len(a))
	for _, v := range a {
		setA[v] = struct{}{}
	}
	intersect := 0
	setB := make(map[string]struct{}, len(b))
	for _, v := range b {
		setB[v] = struct{}{}
		if _, ok := setA[v]; ok {
			intersect++
		}
	}
	union := len(setA) + len(setB) - intersect
	if union == 0 {
		return 0
	}
	return float64(intersect) / float64(union)
}

// pickBest 在候选中挑选保留文件。
func pickBest(files []VideoFile, idxs []int) int {
	best := idxs[0]
	for _, idx := range idxs[1:] {
		if better(files[idx], files[best]) {
			best = idx
		}
	}
	return best
}

// better 定义保留优先级：体积更大、修改时间更新、路径字典序更小。
func better(a, b VideoFile) bool {
	if a.SizeBytes != b.SizeBytes {
		return a.SizeBytes > b.SizeBytes
	}
	if !a.ModTime.Equal(b.ModTime) {
		return a.ModTime.After(b.ModTime)
	}
	return a.Path < b.Path
}

// organize 将保留文件按省份/城市归档，并可选保存重复副本。
func organize(cfg Config, kept []VideoFile, dups []DuplicateRecord) ([]OrganizeRecord, []string) {
	errs := make([]string, 0)
	records := make([]OrganizeRecord, 0, len(kept))
	dupSet := make(map[string]DuplicateRecord, len(dups))
	bar := newProgressBar("视频归档", len(kept))
	defer bar.finish()
	for _, d := range dups {
		dupSet[d.OriginalPath] = d
	}

	for _, f := range kept {
		bar.increment()
		destDir := targetVideoDir(cfg, f)
		if err := ensureDir(destDir); err != nil {
			errs = append(errs, fmt.Sprintf("创建目录失败: %s: %v", destDir, err))
			continue
		}
		destPath, err := uniqueDestPath(destDir, filepath.Base(f.Path))
		if err != nil {
			errs = append(errs, fmt.Sprintf("生成目标路径失败: %s: %v", f.Path, err))
			continue
		}
		if err = transferFile(f.Path, destPath, cfg.CopyMode); err != nil {
			errs = append(errs, fmt.Sprintf("写入归档失败: %s -> %s: %v", f.Path, destPath, err))
			continue
		}
		records = append(records, OrganizeRecord{
			SourcePath: f.Path,
			DestPath:   destPath,
			Province:   f.Province,
			City:       f.City,
		})
	}

	if cfg.KeepDuplicateCopies {
		dupDir := filepath.Join(cfg.OutputDir, "_duplicates")
		if err := ensureDir(dupDir); err != nil {
			errs = append(errs, fmt.Sprintf("创建重复目录失败: %s: %v", dupDir, err))
			return records, errs
		}
		for src := range dupSet {
			name := filepath.Base(src)
			destPath, err := uniqueDestPath(dupDir, name)
			if err != nil {
				errs = append(errs, fmt.Sprintf("重复副本路径失败: %s: %v", src, err))
				continue
			}
			if err = transferFile(src, destPath, true); err != nil {
				errs = append(errs, fmt.Sprintf("复制重复副本失败: %s -> %s: %v", src, destPath, err))
			}
		}
	}

	return records, errs
}

// targetVideoDir 计算视频归档目录；未知省市时按原目录结构放置。
func targetVideoDir(cfg Config, f VideoFile) string {
	if shouldFallbackToOriginalDir(cfg, f) {
		if f.RelativeDir == "" || f.RelativeDir == "." {
			return cfg.OutputDir
		}
		return filepath.Join(cfg.OutputDir, f.RelativeDir)
	}
	return filepath.Join(cfg.OutputDir, f.Province, f.City)
}

// shouldFallbackToOriginalDir 判断是否需要回落到输入原目录结构。
func shouldFallbackToOriginalDir(cfg Config, f VideoFile) bool {
	province := strings.TrimSpace(f.Province)
	city := strings.TrimSpace(f.City)
	if province == "" || city == "" {
		return true
	}
	return province == cfg.UnknownProvince || city == cfg.UnknownCity
}

// organizeNonVideoFiles 非视频文件不参与分析，按原目录结构归档。
func organizeNonVideoFiles(cfg Config, paths []string) ([]OrganizeRecord, []string) {
	records := make([]OrganizeRecord, 0, len(paths))
	errs := make([]string, 0)
	bar := newProgressBar("非视频归档", len(paths))
	defer bar.finish()
	for _, src := range paths {
		bar.increment()
		rel, err := filepath.Rel(cfg.InputDir, src)
		if err != nil {
			errs = append(errs, fmt.Sprintf("计算相对路径失败: %s: %v", src, err))
			continue
		}
		destPath := filepath.Join(cfg.OutputDir, rel)
		destDir := filepath.Dir(destPath)
		if err = ensureDir(destDir); err != nil {
			errs = append(errs, fmt.Sprintf("创建目录失败: %s: %v", destDir, err))
			continue
		}
		if _, statErr := os.Stat(destPath); statErr == nil {
			destPath, err = uniqueDestPath(destDir, filepath.Base(src))
			if err != nil {
				errs = append(errs, fmt.Sprintf("生成目标路径失败: %s: %v", src, err))
				continue
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			errs = append(errs, fmt.Sprintf("检查目标路径失败: %s: %v", destPath, statErr))
			continue
		}
		if err = transferFile(src, destPath, cfg.CopyMode); err != nil {
			errs = append(errs, fmt.Sprintf("归档非视频文件失败: %s -> %s: %v", src, destPath, err))
			continue
		}
		records = append(records, OrganizeRecord{
			SourcePath: src,
			DestPath:   destPath,
		})
	}
	return records, errs
}

// ensureDir 确保目录存在。
func ensureDir(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

// uniqueDestPath 生成不冲突的目标文件路径。
func uniqueDestPath(dir, fileName string) (string, error) {
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

// transferFile 根据模式执行复制或移动。
func transferFile(src, dst string, copyMode bool) error {
	if copyMode {
		return copyFile(src, dst)
	}
	return moveFile(src, dst)
}

// copyFile 复制文件内容并落盘。
func copyFile(src, dst string) error {
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

// moveFile 优先重命名移动，失败时回退为复制后删除。
func moveFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

// writeReports 输出 JSON、CSV 与日志报告。
func writeReports(outDir string, report Report) error {
	if err := writeReportJSON(outDir, report); err != nil {
		return err
	}
	if err := writeDuplicateCSV(outDir, report.DuplicateRecords); err != nil {
		return err
	}
	return writeLogFile(outDir, report)
}

// writeReportJSON 写出结构化 JSON 报告。
func writeReportJSON(outDir string, report Report) error {
	path := filepath.Join(outDir, "report.json")
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// writeDuplicateCSV 写出重复文件清单。
func writeDuplicateCSV(outDir string, records []DuplicateRecord) error {
	path := filepath.Join(outDir, "duplicates.csv")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	if err = w.Write([]string{"original_path", "keep_path", "reason", "similarity"}); err != nil {
		return err
	}
	for _, r := range records {
		row := []string{r.OriginalPath, r.KeepPath, r.Reason, fmt.Sprintf("%.6f", r.Similarity)}
		if err = w.Write(row); err != nil {
			return err
		}
	}
	return w.Error()
}

// writeLogFile 写出文本处理日志。
func writeLogFile(outDir string, report Report) error {
	path := filepath.Join(outDir, "process.log")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	defer bw.Flush()

	_, _ = fmt.Fprintf(bw, "generated_at=%s\n", report.GeneratedAt)
	_, _ = fmt.Fprintf(bw, "input_dir=%s\n", report.InputDir)
	_, _ = fmt.Fprintf(bw, "output_dir=%s\n", report.OutputDir)
	_, _ = fmt.Fprintf(bw, "total_scanned=%d\n", report.TotalScanned)
	_, _ = fmt.Fprintf(bw, "total_unique_kept=%d\n", report.TotalUniqueKept)
	_, _ = fmt.Fprintf(bw, "total_duplicates=%d\n", report.TotalDuplicates)
	if len(report.Errors) > 0 {
		_, _ = fmt.Fprintln(bw, "errors:")
		for _, e := range report.Errors {
			_, _ = fmt.Fprintf(bw, "- %s\n", e)
		}
	}
	return nil
}
