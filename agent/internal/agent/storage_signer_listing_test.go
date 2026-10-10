package agent

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

type listingObject struct {
	Key  *string `xml:"Key,omitempty"`
	Size *int64  `xml:"Size,omitempty"`
}

type listingPage struct {
	XMLName               xml.Name        `xml:"ListBucketResult"`
	IsTruncated           bool            `xml:"IsTruncated"`
	NextContinuationToken string          `xml:"NextContinuationToken,omitempty"`
	Contents              []listingObject `xml:"Contents"`
}

func listedObject(key string, size int64) listingObject {
	return listingObject{Key: &key, Size: &size}
}

// Exercise the AWS SDK's real HTTP listing/decoding. Fixtures are local and use
// deliberately fake credentials; no provider inventory or customer data is read.
func listFixture(t *testing.T, prefix string, serve func(*testing.T, int32, *http.Request) listingPage, requestLimit ...int32) ([]string, error, int32) {
	t.Helper()
	limit := int32(5)
	if len(requestLimit) > 0 {
		limit = requestLimit[0]
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call > limit {
			// Bound a regressed pagination loop independently of its context.
			http.Error(w, "fixture request limit", http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/models" || r.URL.Query().Get("list-type") != "2" {
			t.Errorf("unexpected list request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(serve(t, call, r))
	}))
	defer server.Close()
	src := protocol.BucketRef{Bucket: "models", Prefix: prefix, Endpoint: server.URL, Region: "us-east-1"}
	client := s3ClientFor(aws.Credentials{AccessKeyID: "fixture-key", SecretAccessKey: "test"}, src)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	objects, err := listObjects(ctx, client, src)
	return objects, err, calls.Load()
}

func TestStorageListingSkipsOnlySafeEmptyDirectoryMarkers(t *testing.T) {
	for _, prefix := range []string{"", "weights", "weights/"} {
		t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
			root := prefix
			if root != "" && !strings.HasSuffix(root, "/") {
				root += "/"
			}
			objects, err, calls := listFixture(t, prefix, func(t *testing.T, _ int32, r *http.Request) listingPage {
				if got := r.URL.Query().Get("prefix"); got != root {
					t.Errorf("listing prefix = %q, want signing directory boundary %q", got, root)
				}
				contents := []listingObject{
					listedObject(root+"nested/", 0),
					listedObject(root+"nested/model.bin", 42),
					listedObject(root+"empty.json", 0),
				}
				if root != "" {
					contents = append([]listingObject{listedObject(root, 0)}, contents...)
				}
				return listingPage{Contents: contents}
			})
			want := []string{"nested/model.bin", "empty.json"}
			if err != nil || !reflect.DeepEqual(objects, want) || calls != 1 {
				t.Fatalf("listing = %v, %v, %d calls; want %v in one request", objects, err, calls, want)
			}
		})
	}
}

func TestStorageListingPagination(t *testing.T) {
	objects, err, calls := listFixture(t, "weights", func(t *testing.T, call int32, r *http.Request) listingPage {
		if r.URL.Query().Get("prefix") != "weights/" {
			t.Error("listing does not match joinKey's directory boundary")
		}
		if call == 1 {
			if r.URL.Query().Get("continuation-token") != "" {
				t.Error("first page unexpectedly has a continuation token")
			}
			return listingPage{IsTruncated: true, NextContinuationToken: "next+/= token", Contents: []listingObject{listedObject("weights/a", 1)}}
		}
		if r.URL.Query().Get("continuation-token") != "next+/= token" {
			t.Error("continuation token was not preserved")
		}
		return listingPage{Contents: []listingObject{listedObject("weights/b", 2)}}
	})
	if err != nil || !reflect.DeepEqual(objects, []string{"a", "b"}) || calls != 2 {
		t.Fatalf("listing = %v, %v, calls=%d", objects, err, calls)
	}
}

func TestStorageListingRejectsNonProgressingPagination(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tokens []string
	}{
		{name: "missing token", tokens: []string{""}},
		{name: "repeated token", tokens: []string{"a", "a"}},
		{name: "token cycle", tokens: []string{"a", "b", "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err, calls := listFixture(t, "weights/", func(t *testing.T, call int32, r *http.Request) listingPage {
				i := min(int(call)-1, len(tc.tokens)-1)
				return listingPage{IsTruncated: true, NextContinuationToken: tc.tokens[i]}
			})
			if err == nil || !strings.Contains(err.Error(), "continuation token") || calls != int32(len(tc.tokens)) {
				t.Fatalf("non-progressing listing = %v, %d calls; want token error after %d calls", err, calls, len(tc.tokens))
			}
		})
	}
}

func TestStorageListingRejectsUnsafeOrInconsistentObjects(t *testing.T) {
	for name, contents := range map[string][]listingObject{
		"outside prefix":          {listedObject("elsewhere/model", 1)},
		"sibling prefix":          {listedObject("weights-backup/model", 1)},
		"traversal":               {listedObject("weights/../escape", 1)},
		"unsafe marker":           {listedObject("weights/../escape/", 0)},
		"double slash marker":     {listedObject("weights//dir/", 0)},
		"nonempty trailing slash": {listedObject("weights/dir/", 1)},
		"missing marker size":     {{Key: aws.String("weights/dir/")}},
		"missing key":             {{Size: aws.Int64(1)}},
		"duplicate":               {listedObject("weights/model", 1), listedObject("weights/model", 1)},
	} {
		t.Run(name, func(t *testing.T) {
			objects, err, _ := listFixture(t, "weights/", func(t *testing.T, _ int32, r *http.Request) listingPage {
				return listingPage{Contents: contents}
			})
			if err == nil || objects != nil {
				t.Fatalf("inconsistent listing must fail without partial results: %v, %v", objects, err)
			}
		})
	}
}

func TestStorageListingPageLimit(t *testing.T) {
	objects, err, calls := listFixture(t, "weights/", func(t *testing.T, call int32, r *http.Request) listingPage {
		return listingPage{IsTruncated: true, NextContinuationToken: fmt.Sprint(call)}
	}, maxStorageListPages+1)
	if err == nil || !strings.Contains(err.Error(), "page listing limit") || objects != nil || calls != maxStorageListPages {
		t.Fatalf("unbounded empty pages: objects=%v, err=%v, requests=%d", objects, err, calls)
	}
}

func TestStorageListingObjectLimit(t *testing.T) {
	objects, err, _ := listFixture(t, "weights/", func(t *testing.T, call int32, r *http.Request) listingPage {
		contents := make([]listingObject, maxSignedObjects+1)
		for i := range contents {
			contents[i] = listedObject(fmt.Sprintf("weights/model-%d", i), 1)
		}
		return listingPage{Contents: contents}
	})
	if err == nil || !strings.Contains(err.Error(), "more than 1000 objects") || objects != nil {
		t.Fatalf("over-limit listing returned partial objects: %v, %v", objects, err)
	}
}

func TestStorageListingRejectsDuplicateAcrossPages(t *testing.T) {
	objects, err, calls := listFixture(t, "weights/", func(t *testing.T, call int32, r *http.Request) listingPage {
		return listingPage{
			IsTruncated: call == 1, NextContinuationToken: "next",
			Contents: []listingObject{listedObject("weights/model", 1)},
		}
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate object key") || objects != nil || calls != 2 {
		t.Fatalf("duplicate across pages accepted: %v, %v, calls=%d", objects, err, calls)
	}
}

func TestStorageListingOnlyDirectoryMarkers(t *testing.T) {
	objects, err, _ := listFixture(t, "weights/", func(t *testing.T, _ int32, r *http.Request) listingPage {
		return listingPage{Contents: []listingObject{listedObject("weights/", 0), listedObject("weights/empty/", 0)}}
	})
	if err != nil || objects == nil || len(objects) != 0 {
		t.Fatalf("marker-only prefix should return an explicit empty list: %v, %v", objects, err)
	}
}

func TestStorageListingRequiresPaginationStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><Contents><Key>weights/model</Key><Size>1</Size></Contents></ListBucketResult>`))
	}))
	defer server.Close()
	src := protocol.BucketRef{Bucket: "models", Prefix: "weights/", Endpoint: server.URL, Region: "us-east-1"}
	client := s3ClientFor(aws.Credentials{AccessKeyID: "fixture-key", SecretAccessKey: "test"}, src)
	objects, err := listObjects(context.Background(), client, src)
	if err == nil || !strings.Contains(err.Error(), "pagination status") || objects != nil {
		t.Fatalf("listing claimed complete without pagination status: %v, %v", objects, err)
	}
}
