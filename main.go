package main

import (
	"log"
	"time"

	"deduplicatevideo/internal/app"
	"deduplicatevideo/internal/config"
	"deduplicatevideo/internal/console"
	"deduplicatevideo/internal/fsutil"
	"deduplicatevideo/internal/report"
)

func main() {
	console.EnsureUTF8Console()
	cfg, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("参数错误: %v", err)
	}
	if err = fsutil.EnsureDir(cfg.OutputDir); err != nil {
		log.Fatalf("无法创建输出目录: %v", err)
	}

	start := time.Now()
	result, err := app.Run(cfg)
	if err != nil {
		log.Fatalf("执行失败: %v", err)
	}

	if err = report.WriteReports(cfg.OutputDir, result); err != nil {
		log.Fatalf("写入报告失败: %v", err)
	}

	log.Printf("完成: 扫描=%d, 保留=%d, 重复=%d, 耗时=%s",
		result.TotalScanned, result.TotalUniqueKept, result.TotalDuplicates, time.Since(start).String())
}
