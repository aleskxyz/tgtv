package remux

import "testing"

func TestVideoPadFilterArgsMapsVideoWithoutLogo(t *testing.T) {
	_, args := videoPadFilterArgs(0.04, "", false)
	found := false
	for _, a := range args {
		if a == "0:v:0?" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected -map 0:v:0? in filter args, got %v", args)
	}
}
