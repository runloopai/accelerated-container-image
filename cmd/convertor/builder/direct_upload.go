package builder

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
)

const (
	ociManifestV1MediaType = "application/vnd.oci.image.manifest.v1+json"
	partUploadConcurrency  = 4
)

// CRC-64/NVME lookup table. Go's crc64.MakeTable takes the polynomial in
// reflected form. Go also applies init=0xFFFFFFFFFFFFFFFF and
// xorout=0xFFFFFFFFFFFFFFFF internally (same as the NVMe spec), so
// crc64.New / crc64.Checksum are correct as-is without manual init/xorout.
//
// The polynomial 0x9A6C9329AC4BC9B5 was verified against the CRC RevEng
// catalog check value: crc64.Checksum([]byte("123456789"), table) ==
// 0xAE8B14860A799888, and produces the same output as the Rust
// crc64fast_nvme crate used on the server side.
var crc64NVMETable = crc64.MakeTable(0x9A6C9329AC4BC9B5)

// ---- wire types (mirror discoball/registry/handlers/directupload.go) --------

// prepareDirectUploadRequest is sent to the prepare endpoint.
// The manifest field is Go []byte, which JSON-encodes as base64.
type prepareDirectUploadRequest struct {
	Manifest  []byte `json:"manifest"`
	MediaType string `json:"media_type"`
}

type blobUploadInstruction struct {
	Digest   string           `json:"digest"`
	Exists   bool             `json:"exists"`
	Token    *string          `json:"token,omitempty"`
	Parts    []uploadPartInfo `json:"parts,omitempty"`
	PartSize *int64           `json:"part_size,omitempty"`
}

type uploadPartInfo struct {
	Number int    `json:"number"`
	URL    string `json:"url"`
}

type prepareDirectUploadResponse struct {
	Blobs []blobUploadInstruction `json:"blobs"`
}

type completedPart struct {
	Number int    `json:"number"`
	ETag   string `json:"etag"`
}

type blobConfirmEntry struct {
	Digest    string          `json:"digest"`
	Token     string          `json:"token"`
	Parts     []completedPart `json:"parts"`
	CRC64NVME *string         `json:"crc64nvme,omitempty"`
}

type confirmDirectUploadRequest struct {
	Manifest  []byte             `json:"manifest"`
	MediaType string             `json:"media_type"`
	Tag       *string            `json:"tag,omitempty"`
	Blobs     []blobConfirmEntry `json:"blobs"`
}

// ---- DirectUploadFromStore --------------------------------------------------

// DirectUploadFromStore reads the converted OCI image from the content store
// captured by FileBasedResolver and pushes blobs directly to S3 via discoball's
// prepare/confirm API. Returns the manifest digest stored by discoball.
//
// imageRef must be the full reference (e.g. "disco.runloop.pro/account:bpt_xxx").
// registryURL is the discoball API endpoint (e.g. "https://disco.runloop.pro").
// When provided, its scheme and host are used as the discoball endpoint; when
// empty, the endpoint is derived from the registry host in imageRef over https.
func DirectUploadFromStore(
	ctx context.Context,
	store content.Store,
	imageStore images.Store,
	imageRef string,
	registryURL string,
) (string, error) {
	registry, repository, tag, err := parseImageRef(imageRef)
	if err != nil {
		return "", fmt.Errorf("invalid image ref %q: %w", imageRef, err)
	}
	logrus.Debugf("direct-upload: registry=%s repository=%s tag=%s", registry, repository, tag)

	// Determine the discoball endpoint host and scheme. When --registry-url is
	// provided we use its host (allowing the API endpoint to differ from the
	// image destination host in -r). When omitted we fall back to the registry
	// host from -r over https.
	apiHost := registry
	scheme := "https"
	if registryURL != "" {
		parsed, parseErr := url.Parse(registryURL)
		if parseErr != nil || parsed.Host == "" {
			return "", fmt.Errorf("invalid --registry-url %q: must be an absolute URL with a host (e.g. https://disco.runloop.pro)", registryURL)
		}
		apiHost = parsed.Host
		scheme = parsed.Scheme
	}

	// Find the converted image in the store.
	imgs, err := imageStore.List(ctx, "")
	if err != nil {
		return "", fmt.Errorf("listing image store: %w", err)
	}
	var nonIndex []images.Image
	for _, img := range imgs {
		if img.Target.MediaType == ocispec.MediaTypeImageIndex {
			return "", fmt.Errorf("direct upload does not support multi-arch output (OCI index found); use a single-platform build")
		}
		nonIndex = append(nonIndex, img)
	}
	if len(nonIndex) == 0 {
		return "", fmt.Errorf("no image found in output store")
	}
	if len(nonIndex) > 1 {
		return "", fmt.Errorf("direct upload does not support multi-arch output (multiple manifests found); use a single-platform build")
	}
	manifestDesc := nonIndex[0].Target

	// Read manifest bytes verbatim — reusing the on-disk bytes ensures the
	// manifest digest stored by discoball matches what the content store holds.
	manifestBytes, err := content.ReadBlob(ctx, store, manifestDesc)
	if err != nil {
		return "", fmt.Errorf("reading manifest: %w", err)
	}

	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return "", fmt.Errorf("parsing manifest: %w", err)
	}

	// Transport with per-operation timeouts. http.Client.Timeout is intentionally
	// unset: S3 PUTs can be large and slow to stream. ResponseHeaderTimeout instead
	// bounds the server think-time after all request bytes are sent, catching stuck
	// connections without killing legitimate large-blob uploads mid-stream.
	client := &http.Client{
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSHandshakeTimeout:   30 * time.Second,
			ResponseHeaderTimeout: 5 * time.Minute,
		},
	}
	// repository is interpolated as literal path segments (slashes pass through
	// unencoded), matching the Rust client and the server's wildcard route
	// ({*repo} in axum). In practice repository is always a single-segment
	// account UUID so no slash encoding ambiguity arises.
	baseURL := fmt.Sprintf("%s://%s/gitlab/v1/repositories/%s/direct-upload", scheme, apiHost, repository)

	// Phase 1: prepare.
	prep, err := prepareUpload(ctx, client, baseURL, manifestBytes)
	if err != nil {
		return "", fmt.Errorf("prepare_direct_upload failed: %w", err)
	}
	logrus.Infof("direct-upload prepare: total=%d missing=%d", len(prep.Blobs), countMissing(prep.Blobs))

	// Phase 2: upload missing blobs concurrently.
	// Collect missing blobs first so each goroutine writes to a unique index.
	type missingEntry struct {
		instruction blobUploadInstruction
		idx         int
	}
	var missing []missingEntry
	for i, instruction := range prep.Blobs {
		if instruction.Exists {
			continue
		}
		if instruction.Token == nil {
			return "", fmt.Errorf("prepare response missing token for blob %s", instruction.Digest)
		}
		if len(instruction.Parts) == 0 {
			return "", fmt.Errorf("prepare response has no parts for blob %s", instruction.Digest)
		}
		if instruction.PartSize == nil || *instruction.PartSize <= 0 {
			return "", fmt.Errorf("prepare response has invalid part_size for blob %s", instruction.Digest)
		}
		missing = append(missing, missingEntry{instruction: instruction, idx: i})
	}

	// A single semaphore shared across all blobs keeps total concurrent PUTs
	// bounded at partUploadConcurrency regardless of how many blobs are missing.
	globalSem := make(chan struct{}, partUploadConcurrency)
	confirmBlobs := make([]blobConfirmEntry, len(missing))
	g, gctx := errgroup.WithContext(ctx)
	for i, m := range missing {
		i, m := i, m
		g.Go(func() error {
			token := *m.instruction.Token
			parts := m.instruction.Parts
			partSize := *m.instruction.PartSize

			instDigest := digest.Digest(m.instruction.Digest)
			var completed []completedPart
			var crc64nvme *string

			if instDigest == manifest.Config.Digest {
				// Config blob: small, upload sequentially outside the global sem.
				configBytes, err := content.ReadBlob(gctx, store, manifest.Config)
				if err != nil {
					return fmt.Errorf("reading config blob: %w", err)
				}
				completed, err = uploadPartsFromBytes(gctx, client, configBytes, parts, partSize)
				if err != nil {
					return fmt.Errorf("uploading config parts: %w", err)
				}
				crc := computeCRC64NVME(configBytes)
				crc64nvme = &crc
			} else {
				// Layer blob: stream from content store, parts share the global semaphore.
				var layerDesc *ocispec.Descriptor
				for j := range manifest.Layers {
					if manifest.Layers[j].Digest == instDigest {
						layerDesc = &manifest.Layers[j]
						break
					}
				}
				if layerDesc == nil {
					return fmt.Errorf("discoball requested unknown blob: %s", m.instruction.Digest)
				}
				var err error
				completed, crc64nvme, err = uploadPartsFromStore(gctx, client, store, *layerDesc, parts, partSize, globalSem)
				if err != nil {
					return fmt.Errorf("uploading layer parts (digest=%s): %w", m.instruction.Digest, err)
				}
			}

			confirmBlobs[i] = blobConfirmEntry{
				Digest:    m.instruction.Digest,
				Token:     token,
				Parts:     completed,
				CRC64NVME: crc64nvme,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return "", err
	}

	// Phase 3: confirm.
	manifestDigest, err := confirmUpload(ctx, client, baseURL, manifestBytes, tag, confirmBlobs)
	if err != nil {
		return "", fmt.Errorf("confirm_direct_upload failed: %w", err)
	}
	logrus.Infof("direct-upload complete: manifest_digest=%s", manifestDigest)
	return manifestDigest, nil
}

// ---- HTTP helpers -----------------------------------------------------------

func prepareUpload(ctx context.Context, client *http.Client, baseURL string, manifestBytes []byte) (*prepareDirectUploadResponse, error) {
	reqBody, err := json.Marshal(prepareDirectUploadRequest{
		Manifest:  manifestBytes,
		MediaType: ociManifestV1MediaType,
	})
	if err != nil {
		return nil, err
	}
	resp, err := doPost(ctx, client, baseURL+"/prepare/", reqBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading prepare response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("prepare returned HTTP %d: %s", resp.StatusCode, body)
	}
	var parsed prepareDirectUploadResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("parsing prepare response: %w", err)
	}
	return &parsed, nil
}

func confirmUpload(ctx context.Context, client *http.Client, baseURL string, manifestBytes []byte, tag string, blobs []blobConfirmEntry) (string, error) {
	var tagPtr *string
	if tag != "" {
		tagPtr = &tag
	}
	reqBody, err := json.Marshal(confirmDirectUploadRequest{
		Manifest:  manifestBytes,
		MediaType: ociManifestV1MediaType,
		Tag:       tagPtr,
		Blobs:     blobs,
	})
	if err != nil {
		return "", err
	}
	resp, err := doPost(ctx, client, baseURL+"/confirm/", reqBody)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading confirm response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("confirm returned HTTP %d: %s", resp.StatusCode, body)
	}

	// Prefer the digest from the response header; fall back to computing it locally.
	manifestDigest := resp.Header.Get("Docker-Content-Digest")
	if manifestDigest == "" {
		manifestDigest = digest.FromBytes(manifestBytes).String()
	}
	return manifestDigest, nil
}

func doPost(ctx context.Context, client *http.Client, rawURL string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}

// ---- Part upload helpers ----------------------------------------------------

// uploadPartsFromBytes uploads an in-memory blob via the presigned part URLs.
func uploadPartsFromBytes(ctx context.Context, client *http.Client, data []byte, parts []uploadPartInfo, partSize int64) ([]completedPart, error) {
	completed := make([]completedPart, 0, len(parts))
	for _, p := range parts {
		offset := int64(p.Number-1) * partSize
		end := offset + partSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		etag, err := putPart(ctx, client, p.URL, data[offset:end])
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", p.Number, err)
		}
		completed = append(completed, completedPart{Number: p.Number, ETag: etag})
	}
	sort.Slice(completed, func(i, j int) bool { return completed[i].Number < completed[j].Number })
	return completed, nil
}

// uploadPartsFromStore streams a blob from the content store to S3 presigned URLs.
// Returns completed parts and the base64-encoded CRC64/NVME of the full blob.
func uploadPartsFromStore(
	ctx context.Context,
	client *http.Client,
	store content.Store,
	desc ocispec.Descriptor,
	parts []uploadPartInfo,
	partSize int64,
	sem chan struct{},
) ([]completedPart, *string, error) {
	ra, err := store.ReaderAt(ctx, desc)
	if err != nil {
		return nil, nil, fmt.Errorf("opening blob %s: %w", desc.Digest, err)
	}
	defer ra.Close()

	totalSize := ra.Size()
	type result struct {
		part completedPart
		err  error
	}

	results := make([]result, len(parts))
	var wg sync.WaitGroup

	for i, p := range parts {
		wg.Add(1)
		go func(idx int, p uploadPartInfo) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[idx] = result{err: ctx.Err()}
				return
			}
			defer func() { <-sem }()

			offset := int64(p.Number-1) * partSize
			size := partSize
			if offset+size > totalSize {
				size = totalSize - offset
			}
			buf := make([]byte, size)
			if _, err := ra.ReadAt(buf, offset); err != nil {
				results[idx] = result{err: fmt.Errorf("reading part %d from store: %w", p.Number, err)}
				return
			}
			etag, err := putPart(ctx, client, p.URL, buf)
			if err != nil {
				results[idx] = result{err: fmt.Errorf("part %d: %w", p.Number, err)}
				return
			}
			results[idx] = result{part: completedPart{Number: p.Number, ETag: etag}}
		}(i, p)
	}
	wg.Wait()

	completed := make([]completedPart, 0, len(parts))
	for _, r := range results {
		if r.err != nil {
			return nil, nil, r.err
		}
		completed = append(completed, r.part)
	}
	sort.Slice(completed, func(i, j int) bool { return completed[i].Number < completed[j].Number })

	// Compute CRC64/NVME by reading the full blob sequentially.
	crcHash := crc64.New(crc64NVMETable)
	if _, err := io.Copy(crcHash, io.NewSectionReader(ra, 0, totalSize)); err != nil {
		return nil, nil, fmt.Errorf("computing CRC64/NVME: %w", err)
	}
	var crcBuf [8]byte
	binary.BigEndian.PutUint64(crcBuf[:], crcHash.Sum64())
	crcStr := base64.StdEncoding.EncodeToString(crcBuf[:])

	return completed, &crcStr, nil
}

// putPart PUTs buf to the presigned URL and returns the ETag.
// Retries up to 3 times on transient (5xx) errors.
func putPart(ctx context.Context, client *http.Client, presignedURL string, buf []byte) (string, error) {
	const maxRetries = 3
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(100<<(attempt-1)) * time.Millisecond // 100ms, 200ms, 400ms
			logrus.Warnf("retrying S3 part upload (attempt=%d delay=%s): %v", attempt, delay, lastErr)
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, bytes.NewReader(buf))
		if err != nil {
			return "", err
		}
		req.ContentLength = int64(len(buf))

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode/100 == 2 {
			etag := resp.Header.Get("ETag")
			if etag == "" {
				return "", fmt.Errorf("S3 part upload returned no ETag")
			}
			return etag, nil
		}
		if resp.StatusCode/100 == 5 && attempt < maxRetries {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
			continue
		}
		return "", fmt.Errorf("S3 part upload HTTP %d: %s", resp.StatusCode, body)
	}
	return "", fmt.Errorf("S3 part upload failed after %d retries: %w", maxRetries, lastErr)
}

// ---- Utilities --------------------------------------------------------------

func countMissing(blobs []blobUploadInstruction) int {
	n := 0
	for _, b := range blobs {
		if !b.Exists {
			n++
		}
	}
	return n
}

func computeCRC64NVME(data []byte) string {
	crcVal := crc64.Checksum(data, crc64NVMETable)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], crcVal)
	return base64.StdEncoding.EncodeToString(buf[:])
}

// parseImageRef parses "registry/repository:tag" into its components.
// The repository may contain slashes (e.g. "host/org/repo:tag").
func parseImageRef(ref string) (registry, repository, tag string, err error) {
	// Strip scheme if present (shouldn't be, but be defensive).
	ref = strings.TrimPrefix(ref, "https://")
	ref = strings.TrimPrefix(ref, "http://")

	slashIdx := strings.Index(ref, "/")
	if slashIdx < 0 {
		return "", "", "", fmt.Errorf("missing repository in ref %q", ref)
	}
	registry = ref[:slashIdx]
	rest := ref[slashIdx+1:]

	// Tag is after the last ':' in rest, but only if ':' comes after any '/'.
	colonIdx := strings.LastIndex(rest, ":")
	if colonIdx >= 0 {
		repository = rest[:colonIdx]
		tag = rest[colonIdx+1:]
	} else {
		repository = rest
	}
	if repository == "" {
		return "", "", "", fmt.Errorf("empty repository in ref %q", ref)
	}

	// Validate: no URL characters that would break the path segment.
	if _, parseErr := url.ParseRequestURI("https://" + registry + "/" + repository); parseErr != nil {
		return "", "", "", fmt.Errorf("invalid ref %q: %w", ref, parseErr)
	}
	return registry, repository, tag, nil
}
