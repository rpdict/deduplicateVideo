package app

import (
	"time"

	"deduplicatevideo/internal/dedupe"
	"deduplicatevideo/internal/hash"
	"deduplicatevideo/internal/model"
	"deduplicatevideo/internal/plan"
	"deduplicatevideo/internal/scan"
)

func Run(cfg model.Config) (model.Report, error) {
	if cfg.Step == "move" {
		movePlan, err := plan.ReadMovePlan(cfg.PlanJSONPath)
		if err != nil {
			return model.Report{}, err
		}
		errs := plan.ExecuteMovePlan(cfg, movePlan.Items)
		report := model.Report{
			GeneratedAt: time.Now().Format(time.RFC3339),
			InputDir:    cfg.InputDir,
			OutputDir:   cfg.OutputDir,
		}
		report.Errors = append(report.Errors, errs...)
		return report, nil
	}

	paths, infos, walkErrs := scan.ScanVideoFiles(cfg.InputDir)
	otherPaths, otherScanErrs := scan.ScanNonVideoFiles(cfg.InputDir)
	files, hashErrs := hash.BuildVideoFiles(cfg, paths, infos)

	report := model.Report{
		GeneratedAt:  time.Now().Format(time.RFC3339),
		InputDir:     cfg.InputDir,
		OutputDir:    cfg.OutputDir,
		TotalScanned: len(paths) + len(otherPaths),
	}
	report.Errors = append(report.Errors, walkErrs...)
	report.Errors = append(report.Errors, otherScanErrs...)
	report.Errors = append(report.Errors, hashErrs...)

	kept, dupRecords := dedupe.Deduplicate(files, cfg.NearDupThreshold, cfg.MaxSizeDiffRatio)
	report.DuplicateRecords = dupRecords
	report.TotalDuplicates = len(dupRecords)
	report.TotalUniqueKept = len(kept)

	movePlan, planErrs := plan.BuildMovePlan(cfg, kept, dupRecords, otherPaths)
	report.Errors = append(report.Errors, planErrs...)
	if err := plan.WriteMovePlanJSON(cfg.PlanJSONPath, movePlan); err != nil {
		report.Errors = append(report.Errors, err.Error())
	}
	if err := plan.WriteMovePlanCSV(cfg.PlanCSVPath, movePlan.Items); err != nil {
		report.Errors = append(report.Errors, err.Error())
	}

	if cfg.ConfirmMove {
		if plan.ConfirmMove() {
			for i := range movePlan.Items {
				movePlan.Items[i].Approved = true
			}
			_ = plan.WriteMovePlanJSON(cfg.PlanJSONPath, movePlan)
			_ = plan.WriteMovePlanCSV(cfg.PlanCSVPath, movePlan.Items)
			errs := plan.ExecuteMovePlan(cfg, movePlan.Items)
			report.Errors = append(report.Errors, errs...)
		}
	}

	return report, nil
}
