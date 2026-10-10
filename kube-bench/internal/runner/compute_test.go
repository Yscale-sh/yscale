package runner

import (
	"reflect"
	"testing"
)

func TestComputeSteps(t *testing.T) {
	result := computeSteps([]int{1, 2, 4, 0}, 3)
	expected := []int{1, 2, 3}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
}
