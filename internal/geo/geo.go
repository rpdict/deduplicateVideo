package geo

import (
	"path/filepath"
	"strings"

	"deduplicatevideo/internal/model"
)

func ParseProvinceCity(cfg model.Config, absPath string) (string, string, string) {
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
		return dir, NormalizeGeo(clean[0], cfg.UnknownProvince), cfg.UnknownCity
	}
	return dir, NormalizeGeo(clean[0], cfg.UnknownProvince), NormalizeGeo(clean[1], cfg.UnknownCity)
}

func NormalizeGeo(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	v = strings.ReplaceAll(v, "\\", "_")
	v = strings.ReplaceAll(v, "/", "_")
	return v
}
