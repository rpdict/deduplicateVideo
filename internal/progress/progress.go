package progress

import (
	"fmt"
	"strings"
)

type Bar struct {
	label   string
	total   int
	current int
	width   int
}

func NewBar(label string, total int) *Bar {
	if total <= 0 {
		return nil
	}
	p := &Bar{
		label: label,
		total: total,
		width: 32,
	}
	p.render(false)
	return p
}

func (p *Bar) Increment() {
	if p == nil {
		return
	}
	if p.current < p.total {
		p.current++
	}
	p.render(false)
}

func (p *Bar) Finish() {
	if p == nil {
		return
	}
	p.current = p.total
	p.render(true)
}

func (p *Bar) render(final bool) {
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
