package worker

import "testing"

func TestParseFIO(t *testing.T) {
	data := []byte(`{"jobs":[{"read":{"bw_bytes":1000000000,"iops":250000,"clat_ns":{"percentile":{"50.000000":1000,"95.000000":2000,"99.000000":3000}}},"write":{"bw_bytes":500000000,"iops":125000,"clat_ns":{"percentile":{"50.000000":2000,"95.000000":4000,"99.000000":6000}}},"sync":{"lat_ns":{"percentile":{"99.000000":8000}}}}]}`)
	result, err := parseFIO(fioProfile{Name: "mixed", RW: "randrw", Fsync: true}, "io_uring", true, data)
	if err != nil {
		t.Fatal(err)
	}
	if result.ReadMBps != 1000 || result.WriteMBps != 500 || result.ReadP99MS != 0.003 || result.FsyncP99MS != 0.008 {
		t.Fatalf("unexpected result: %+v", result)
	}
}
