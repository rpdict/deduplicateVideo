package model

import "time"

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

type DuplicateRecord struct {
	OriginalPath string  `json:"original_path"`
	KeepPath     string  `json:"keep_path"`
	Reason       string  `json:"reason"`
	Similarity   float64 `json:"similarity"`
}

type OrganizeRecord struct {
	SourcePath string `json:"source_path"`
	DestPath   string `json:"dest_path"`
	Province   string `json:"province"`
	City       string `json:"city"`
}

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
	SourcePath  string  `json:"source_path"`
	DestPath    string  `json:"dest_path"`
	Action      string  `json:"action"`
	Approved    bool    `json:"approved"`
	Reason      string  `json:"reason"`
	Environment string  `json:"environment"`
	Confidence  float64 `json:"confidence"`
	FramePath   string  `json:"frame_path"`
}

type AIResult struct {
	Environment string   `json:"environment"`
	Confidence  float64  `json:"confidence"`
	Summary     string   `json:"summary"`
	Tags        []string `json:"tags"`
}
