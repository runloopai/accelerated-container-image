/*
   Copyright The Accelerated Container Image Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package builder

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"hash/crc64"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestPrepareRequestManifestBase64Encodes(t *testing.T) {
	body := prepareDirectUploadRequest{
		Manifest:  []byte("hello"),
		MediaType: ociManifestV1MediaType,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Go encodes []byte as base64; base64("hello") = "aGVsbG8="
	var got string
	if err := json.Unmarshal(m["manifest"], &got); err != nil {
		t.Fatalf("unmarshal manifest field: %v", err)
	}
	if want := base64.StdEncoding.EncodeToString([]byte("hello")); got != want {
		t.Errorf("manifest = %q, want %q", got, want)
	}
	var mediaType string
	if err := json.Unmarshal(m["media_type"], &mediaType); err != nil {
		t.Fatalf("unmarshal media_type: %v", err)
	}
	if mediaType != ociManifestV1MediaType {
		t.Errorf("media_type = %q, want %q", mediaType, ociManifestV1MediaType)
	}
}

func TestConfirmRequestOmitsTagWhenEmpty(t *testing.T) {
	body := confirmDirectUploadRequest{
		Manifest:  []byte("m"),
		MediaType: ociManifestV1MediaType,
		Tag:       nil,
		Blobs:     nil,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	json.Unmarshal(raw, &m)
	if _, ok := m["tag"]; ok {
		t.Error("tag field should be omitted when nil")
	}
}

func TestConfirmRequestIncludesTagWhenSet(t *testing.T) {
	tag := "snp_abc"
	body := confirmDirectUploadRequest{
		Manifest:  []byte("m"),
		MediaType: ociManifestV1MediaType,
		Tag:       &tag,
		Blobs:     nil,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	json.Unmarshal(raw, &m)
	var got string
	if err := json.Unmarshal(m["tag"], &got); err != nil {
		t.Fatalf("unmarshal tag: %v", err)
	}
	if got != tag {
		t.Errorf("tag = %q, want %q", got, tag)
	}
}

func TestPrepareResponseParsesExistingAndPendingBlobs(t *testing.T) {
	raw := `{
		"blobs": [
			{"digest": "sha256:aaa", "exists": true},
			{"digest": "sha256:bbb", "size": 12345, "token": "abc.def",
			 "parts": [{"number": 1, "url": "https://s3/?sig=1"}],
			 "part_size": 5242880}
		]
	}`
	var resp prepareDirectUploadResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Blobs) != 2 {
		t.Fatalf("blobs len = %d, want 2", len(resp.Blobs))
	}
	if !resp.Blobs[0].Exists {
		t.Error("blobs[0].exists should be true")
	}
	if resp.Blobs[0].Token != nil {
		t.Error("blobs[0].token should be nil")
	}
	if resp.Blobs[1].Exists {
		t.Error("blobs[1].exists should be false")
	}
	if resp.Blobs[1].Token == nil || *resp.Blobs[1].Token != "abc.def" {
		t.Errorf("blobs[1].token = %v, want \"abc.def\"", resp.Blobs[1].Token)
	}
	if resp.Blobs[1].PartSize == nil || *resp.Blobs[1].PartSize != 5_242_880 {
		t.Errorf("blobs[1].part_size = %v, want 5242880", resp.Blobs[1].PartSize)
	}
	if len(resp.Blobs[1].Parts) != 1 {
		t.Fatalf("blobs[1].parts len = %d, want 1", len(resp.Blobs[1].Parts))
	}
	if resp.Blobs[1].Parts[0].Number != 1 || resp.Blobs[1].Parts[0].URL != "https://s3/?sig=1" {
		t.Errorf("blobs[1].parts[0] = %+v", resp.Blobs[1].Parts[0])
	}
}

func TestParseImageRef(t *testing.T) {
	tests := []struct {
		input      string
		registry   string
		repository string
		tag        string
		wantErr    bool
	}{
		{
			"disco.runloop.ai/repo/foo:snp_x",
			"disco.runloop.ai", "repo/foo", "snp_x", false,
		},
		{
			"disco.runloop.ai/account:bpt_abc",
			"disco.runloop.ai", "account", "bpt_abc", false,
		},
		{
			"disco.runloop.ai/repo/foo",
			"disco.runloop.ai", "repo/foo", "", false,
		},
		{"no-slash", "", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			reg, repo, tag, err := parseImageRef(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if reg != tt.registry || repo != tt.repository || tag != tt.tag {
				t.Errorf("got (%q, %q, %q), want (%q, %q, %q)",
					reg, repo, tag, tt.registry, tt.repository, tt.tag)
			}
		})
	}
}

func TestCRC64NVMECheckVector(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantHex uint64
	}{
		// Standard check value from the CRC RevEng catalog for CRC-64/NVME.
		{"check vector", []byte("123456789"), 0xAE8B14860A799888},
		// Empty input: init XOR xorout = 0xFFFF... XOR 0xFFFF... = 0.
		{"empty", []byte{}, 0x0000000000000000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := crc64.Checksum(tt.input, crc64NVMETable)
			if got != tt.wantHex {
				t.Errorf("CRC-64/NVME(%q) = 0x%016X, want 0x%016X", tt.input, got, tt.wantHex)
			}
		})
	}
}

func TestComputeCRC64NVMEEncoding(t *testing.T) {
	// Pins the full on-wire encoding: big-endian uint64 → standard base64.
	// A little-endian uint64 of 0xAE8B14860A799888 produces "iJh5mBS..."
	// instead of "rosU...", so this catches byte-order bugs that the hex
	// check-vector test above cannot.
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		// 0xAE8B14860A799888 BE → [AE 8B 14 86 0A 79 98 88] → "rosUhgp5mIg="
		{"check vector", []byte("123456789"), "rosUhgp5mIg="},
		// 0x0000000000000000 BE → [00 00 00 00 00 00 00 00] → "AAAAAAAAAAA=" (8 bytes = 12 base64 chars)
		{"empty", []byte{}, "AAAAAAAAAAA="},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeCRC64NVME(tt.input)
			if got != tt.want {
				t.Errorf("computeCRC64NVME(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestPutPart5xxRetrySucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("ETag", `"etag-abc"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	etag, err := putPart(context.Background(), &http.Client{}, srv.URL, []byte("data"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if etag != `"etag-abc"` {
		t.Errorf("etag = %q, want %q", etag, `"etag-abc"`)
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("server received %d requests, want 3 (1 initial + 2 retries)", n)
	}
}

func TestPutPartMissingETag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // no ETag header
	}))
	defer srv.Close()

	_, err := putPart(context.Background(), &http.Client{}, srv.URL, []byte("data"))
	if err == nil {
		t.Fatal("expected error for missing ETag, got nil")
	}
}

func TestPutPartExhaustsRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := putPart(context.Background(), &http.Client{}, srv.URL, []byte("data"))
	if err == nil {
		t.Fatal("expected error after exhausted retries, got nil")
	}
	// maxRetries=3 means attempts 0,1,2,3 — four total requests.
	if n := calls.Load(); n != 4 {
		t.Errorf("server received %d requests, want 4 (1 initial + 3 retries)", n)
	}
}
