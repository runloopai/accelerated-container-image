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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// ExportContentStoreToDir writes converted OCI image artifacts from the content
// store to outputDir in a flat layout for consumption by blueprint-uploader.
//
// Output layout:
//
//	manifest.json       - OCI manifest JSON
//	config.json         - OCI image config JSON
//	config.digest       - "sha256:<hex>" of config.json
//	blobs/<sha256-hex>  - layer blobs, one file per layer (named by digest with
//	                      ':' replaced by '-', e.g. "sha256-abc123...")
//
// Only single-arch OCI manifests are supported; multi-arch index entries are skipped.
func ExportContentStoreToDir(ctx context.Context, store content.Store, imageStore images.Store, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return errors.Wrapf(err, "failed to create output directory %q", outputDir)
	}

	imgs, err := imageStore.List(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to list images in output store")
	}
	if len(imgs) == 0 {
		return errors.New("no images in output store after conversion")
	}

	// Find the first non-index image (for overlaybd OCI base builds there is one per arch).
	var manifestDesc v1.Descriptor
	for _, img := range imgs {
		if img.Target.MediaType != v1.MediaTypeImageIndex &&
			img.Target.MediaType != "application/vnd.docker.distribution.manifest.list.v2+json" {
			manifestDesc = img.Target
			break
		}
	}
	if manifestDesc.Digest == "" {
		return errors.New("no single-arch manifest found in output store")
	}

	// Read and write manifest.json.
	manifestBytes, err := content.ReadBlob(ctx, store, manifestDesc)
	if err != nil {
		return errors.Wrapf(err, "failed to read manifest blob")
	}
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), manifestBytes, 0644); err != nil {
		return errors.Wrapf(err, "failed to write manifest.json")
	}
	log.G(ctx).Debugf("dir exporter: wrote manifest.json (%d bytes)", len(manifestBytes))

	// Parse manifest to find config and layer descriptors.
	var manifest struct {
		Config struct {
			MediaType string `json:"mediaType"`
			Digest    string `json:"digest"`
			Size      int64  `json:"size"`
		} `json:"config"`
		Layers []struct {
			MediaType string `json:"mediaType"`
			Digest    string `json:"digest"`
			Size      int64  `json:"size"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return errors.Wrapf(err, "failed to parse manifest JSON")
	}

	// Write config.json and config.digest.
	configDigestStr := manifest.Config.Digest
	cd, err := digest.Parse(configDigestStr)
	if err != nil {
		return errors.Wrapf(err, "failed to parse config digest %q", configDigestStr)
	}
	configSpec := v1.Descriptor{
		MediaType: manifest.Config.MediaType,
		Digest:    cd,
		Size:      manifest.Config.Size,
	}
	configBytes, err := content.ReadBlob(ctx, store, configSpec)
	if err != nil {
		return errors.Wrapf(err, "failed to read config blob")
	}
	if err := os.WriteFile(filepath.Join(outputDir, "config.json"), configBytes, 0644); err != nil {
		return errors.Wrapf(err, "failed to write config.json")
	}
	if err := os.WriteFile(filepath.Join(outputDir, "config.digest"), []byte(configDigestStr), 0644); err != nil {
		return errors.Wrapf(err, "failed to write config.digest")
	}
	log.G(ctx).Debugf("dir exporter: wrote config.json (digest: %s)", configDigestStr)

	// Write each layer blob to blobs/<sha256-hex>.
	if len(manifest.Layers) == 0 {
		return errors.New("manifest has no layers")
	}
	blobsDir := filepath.Join(outputDir, "blobs")
	if err := os.MkdirAll(blobsDir, 0755); err != nil {
		return errors.Wrapf(err, "failed to create blobs directory")
	}

	for i, layer := range manifest.Layers {
		layerDigestStr := layer.Digest
		ld, err := digest.Parse(layerDigestStr)
		if err != nil {
			return errors.Wrapf(err, "failed to parse layer[%d] digest %q", i, layerDigestStr)
		}
		layerSpec := v1.Descriptor{
			MediaType: layer.MediaType,
			Digest:    ld,
			Size:      layer.Size,
		}
		// Name the file by digest with ':' replaced by '-' for filesystem safety.
		blobFilename := strings.ReplaceAll(layerDigestStr, ":", "-")
		blobPath := filepath.Join(blobsDir, blobFilename)
		if err := copyBlobToFile(ctx, store, layerSpec, blobPath); err != nil {
			return errors.Wrapf(err, "failed to write layer[%d] blob (digest: %s)", i, layerDigestStr)
		}
		log.G(ctx).Debugf("dir exporter: wrote blobs/%s (%s bytes)", blobFilename, fmt.Sprintf("%d", layer.Size))
	}

	log.G(ctx).Infof("dir exporter: wrote %d layer blob(s) to %s", len(manifest.Layers), outputDir)
	return nil
}

// copyBlobToFile streams the blob identified by desc from store to the file at path.
func copyBlobToFile(ctx context.Context, store content.Store, desc v1.Descriptor, path string) error {
	ra, err := store.ReaderAt(ctx, desc)
	if err != nil {
		return errors.Wrapf(err, "failed to open blob %s for reading", desc.Digest)
	}
	defer ra.Close()

	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "failed to create file %q", path)
	}
	defer f.Close()

	_, err = io.Copy(f, content.NewReader(ra))
	return errors.Wrapf(err, "failed to stream blob to %q", path)
}
