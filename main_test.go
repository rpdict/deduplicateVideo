package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"deduplicatevideo/internal/dedupe"
	"deduplicatevideo/internal/geo"
	"deduplicatevideo/internal/hash"
	"deduplicatevideo/internal/model"
	"deduplicatevideo/internal/plan"
)

func TestParseProvinceCity(t *testing.T) {
	cfg := model.Config{
		InputDir:        filepath.Join("D:", "data", "videos"),
		UnknownProvince: "待分类",
		UnknownCity:     "未知城市",
	}
	path := filepath.Join(cfg.InputDir, "浙江省", "杭州市", "a.mp4")
	_, p, c := geo.ParseProvinceCity(cfg, path)
	if p != "浙江省" || c != "杭州市" {
		t.Fatalf("解析失败: province=%s city=%s", p, c)
	}
}

func TestSampleSimilarity(t *testing.T) {
	a := []string{"1", "2", "3"}
	b := []string{"2", "3", "4"}
	got := dedupe.SampleSimilarity(a, b)
	if got < 0.49 || got > 0.51 {
		t.Fatalf("相似度异常: %f", got)
	}
}

func TestDeduplicateExactHash(t *testing.T) {
	now := time.Now()
	files := []model.VideoFile{
		{Path: "a.mp4", SHA256: "x", SizeBytes: 100, ModTime: now, SampleHash: []string{"1", "2"}},
		{Path: "b.mp4", SHA256: "x", SizeBytes: 99, ModTime: now.Add(-time.Minute), SampleHash: []string{"1", "2"}},
		{Path: "c.mp4", SHA256: "y", SizeBytes: 80, ModTime: now, SampleHash: []string{"3", "4"}},
	}
	kept, dups := dedupe.Deduplicate(files, 0.8, 0.2)
	if len(kept) != 2 {
		t.Fatalf("保留数量错误: %d", len(kept))
	}
	if len(dups) != 1 {
		t.Fatalf("重复数量错误: %d", len(dups))
	}
	if dups[0].Reason != "exact_hash" {
		t.Fatalf("重复原因错误: %s", dups[0].Reason)
	}
}

func TestFileSHA256(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.bin")
	data := []byte("hello")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	sum, err := hash.FileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	if sum != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
		t.Fatalf("hash错误: %s", sum)
	}
}

func TestTargetVideoDirUnknownFallsBackToRelativeDir(t *testing.T) {
	cfg := model.Config{
		OutputDir:       filepath.Join("D:", "out"),
		UnknownProvince: "待分类",
		UnknownCity:     "未知城市",
	}
	video := model.VideoFile{
		RelativeDir: "浙江省",
		Province:    "浙江省",
		City:        "未知城市",
	}
	got := plan.TargetVideoDir(cfg, video)
	want := filepath.Join(cfg.OutputDir, "浙江省")
	if got != want {
		t.Fatalf("目录不符合预期: got=%s want=%s", got, want)
	}
}

func TestShouldFallbackToOriginalDir(t *testing.T) {
	cfg := model.Config{
		UnknownProvince: "待分类",
		UnknownCity:     "未知城市",
	}
	if !plan.ShouldFallbackToOriginalDir(cfg, model.VideoFile{Province: "浙江省", City: "未知城市"}) {
		t.Fatalf("未知城市应回落原目录")
	}
	if plan.ShouldFallbackToOriginalDir(cfg, model.VideoFile{Province: "浙江省", City: "杭州市"}) {
		t.Fatalf("已识别省市不应回落原目录")
	}
}

func TestOrganizeNonVideoFilesKeepRelativePath(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	srcDir := filepath.Join(inputDir, "浙江省", "杭州市")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(srcDir, "meta.txt")
	if err := os.WriteFile(src, []byte("meta"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := model.Config{
		InputDir:  inputDir,
		OutputDir: outputDir,
		CopyMode:  true,
	}
	records, errs := plan.OrganizeNonVideoFiles(cfg, []string{src})
	if len(errs) != 0 {
		t.Fatalf("出现错误: %v", errs)
	}
	if len(records) != 1 {
		t.Fatalf("记录数量错误: %d", len(records))
	}
	expected := filepath.Join(outputDir, "浙江省", "杭州市", "meta.txt")
	if records[0].DestPath != expected {
		t.Fatalf("目标路径错误: got=%s want=%s", records[0].DestPath, expected)
	}
	if _, err := os.Stat(expected); err != nil {
		t.Fatalf("目标文件不存在: %v", err)
	}
}
