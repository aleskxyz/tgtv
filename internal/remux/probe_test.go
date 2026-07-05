package remux_test

import (
	"math"
	"testing"

	"github.com/aleskxyz/tgtv/internal/remux"
)

func TestVideoPadFromDuration(t *testing.T) {
	tests := []struct {
		video float64
		want  float64
	}{
		{0.960, 0.040},
		{0, 0.040},
		{0.850, 0.150},
		{0.700, 0.200},
		{1.010, 0},
	}
	for _, tc := range tests {
		got := remux.VideoPadFromDuration(tc.video)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("video=%g pad=%g want %g", tc.video, got, tc.want)
		}
	}
}
