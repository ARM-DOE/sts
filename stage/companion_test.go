package stage

import (
	"testing"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
)

func TestPartSearch(t *testing.T) {
	ranges := [][]int64{
		[]int64{0, 10},
		[]int64{40, 50},
		[]int64{25, 35},
		[]int64{10, 25},
	}
	cmp := &sts.Partial{}
	for _, r := range ranges {
		cmp.Parts = append(cmp.Parts, &sts.ByteRange{
			Beg: r[0],
			End: r[1],
		})
	}
	should := [][]int64{
		[]int64{0, 10},
		[]int64{7, 13},
		[]int64{10, 25},
	}
	shouldnt := [][]int64{
		[]int64{36, 45},
		[]int64{23, 42},
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
		[]int64{40, 50},
		[]int64{40, 55},
		[]int64{0, 10},
		[]int64{25, 35},
		[]int64{10, 25},
		[]int64{30, 45},
	}
	cmp := &sts.Partial{}
	for _, r := range ranges {
		addCompanionPart(cmp, r[0], r[1])
	}
	if len(cmp.Parts) != 3 {
		t.Fatalf("Expected 3 ranges but got: %d", len(cmp.Parts))
	}
	for i, r := range cmp.Parts[1:] {
		prev := cmp.Parts[i]
		if r.Beg < prev.Beg {
			t.Fatalf("Bad order: %d <- %d", r.Beg, cmp.Parts[i].Beg)
		}
	}
}
