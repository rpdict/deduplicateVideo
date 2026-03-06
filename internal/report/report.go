package report

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"deduplicatevideo/internal/model"
)

func WriteReports(outDir string, report model.Report) error {
	if err := WriteReportJSON(outDir, report); err != nil {
		return err
	}
	if err := WriteDuplicateCSV(outDir, report.DuplicateRecords); err != nil {
		return err
	}
	return WriteLogFile(outDir, report)
}

func WriteReportJSON(outDir string, report model.Report) error {
	path := filepath.Join(outDir, "report.json")
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func WriteDuplicateCSV(outDir string, records []model.DuplicateRecord) error {
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

func WriteLogFile(outDir string, report model.Report) error {
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
