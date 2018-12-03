package stage

import (
	"testing"

	"code.arm.gov/dataflow/sts"
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
