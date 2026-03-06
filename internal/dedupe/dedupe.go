package dedupe

import (
	"sort"

	"deduplicatevideo/internal/model"
	"deduplicatevideo/internal/progress"
)

type disjointSet struct {
	parent []int
	rank   []int
}

func newDisjointSet(n int) *disjointSet {
	parent := make([]int, n)
	rank := make([]int, n)
	for i := 0; i < n; i++ {
		parent[i] = i
	}
	return &disjointSet{parent: parent, rank: rank}
}

func (d *disjointSet) find(x int) int {
	if d.parent[x] != x {
		d.parent[x] = d.find(d.parent[x])
	}
	return d.parent[x]
}

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

func Deduplicate(files []model.VideoFile, nearThreshold, maxSizeDiffRatio float64) ([]model.VideoFile, []model.DuplicateRecord) {
	if len(files) == 0 {
		return nil, nil
	}
	byHash := make(map[string][]int)
	for i := range files {
		byHash[files[i].SHA256] = append(byHash[files[i].SHA256], i)
	}

	keptFlag := make([]bool, len(files))
	dupRecords := make([]model.DuplicateRecord, 0)
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
			dupRecords = append(dupRecords, model.DuplicateRecord{
				OriginalPath: files[idx].Path,
				KeepPath:     files[keep].Path,
				Reason:       "exact_hash",
				Similarity:   1.0,
			})
		}
	}

	ds := newDisjointSet(len(uniqueCandidates))
	totalPairs := len(uniqueCandidates) * (len(uniqueCandidates) - 1) / 2
	compareBar := progress.NewBar("近重复比对", totalPairs)
	defer compareBar.Finish()
	for i := 0; i < len(uniqueCandidates); i++ {
		for j := i + 1; j < len(uniqueCandidates); j++ {
			compareBar.Increment()
			a := files[uniqueCandidates[i]]
			b := files[uniqueCandidates[j]]
			if !withinSizeDiff(a.SizeBytes, b.SizeBytes, maxSizeDiffRatio) {
				continue
			}
			sim := SampleSimilarity(a.SampleHash, b.SampleHash)
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

	finalKept := make([]model.VideoFile, 0, len(clusterMap))
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
			dupRecords = append(dupRecords, model.DuplicateRecord{
				OriginalPath: files[idx].Path,
				KeepPath:     files[keep].Path,
				Reason:       "near_duplicate",
				Similarity:   SampleSimilarity(files[idx].SampleHash, files[keep].SampleHash),
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

func SampleSimilarity(a, b []string) float64 {
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

func pickBest(files []model.VideoFile, idxs []int) int {
	best := idxs[0]
	for _, idx := range idxs[1:] {
		if better(files[idx], files[best]) {
			best = idx
		}
	}
	return best
}

func better(a, b model.VideoFile) bool {
	if a.SizeBytes != b.SizeBytes {
		return a.SizeBytes > b.SizeBytes
	}
	if !a.ModTime.Equal(b.ModTime) {
		return a.ModTime.After(b.ModTime)
	}
	return a.Path < b.Path
}
