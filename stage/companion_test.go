package stage

import (
	"testing"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
)

func TestPartSearch(t *testing.T) {
	ranges := [][]int64{
		{0, 10},
		{40, 50},
		{25, 35},
		{10, 25},
	}
	cmp := &sts.Partial{}
	for _, r := range ranges {
		cmp.Parts = append(cmp.Parts, &sts.ByteRange{
			Beg: r[0],
			End: r[1],
		})
	}
	should := [][]int64{
		{0, 10},
		{7, 13},
		{10, 25},
	}
	shouldnt := [][]int64{
		{36, 45},
		{23, 42},
	}
	for _, r := range should {
		if !companionPartExists(cmp, r[0], r[1]) {
			t.Errorf("Should exist: %d - %d", r[0], r[1])
		}
	}
	for _, r := range shouldnt {
		if companionPartExists(cmp, r[0], r[1]) {
			t.Errorf("Should not exist: %d - %d", r[0], r[1])
		}
	}
}

func TestPartAdd(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	ranges := [][]int64{
		{419, 524},
		{524, 629},
		{629, 679},
		{0, 104},
		{104, 209},
		{103, 104},
		{0, 103},
		{0, 43},
		{43, 104},
		{104, 209},
		{205, 209},
		{104, 205},
		{209, 314},
	}
	cmp := &sts.Partial{}
	for _, r := range ranges {
		addCompanionPart(cmp, r[0], r[1])
	}
	if len(cmp.Parts) != 8 {
		t.Fatalf("Expected 8 ranges but got: %d", len(cmp.Parts))
	}
	for i, r := range cmp.Parts[1:] {
		prev := cmp.Parts[i]
		if r.Beg < prev.Beg {
			t.Fatalf("Bad order: %d <- %d", r.Beg, cmp.Parts[i].Beg)
		}
	}
}
