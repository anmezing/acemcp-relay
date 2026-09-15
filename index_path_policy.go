package main

import (
	_ "embed"
	"encoding/json"
	"path"
	"strings"
)

// Snapshot of LCE docs/contracts/cloud-index-path-policy.json. Keep both copies
// identical. Both runtimes use this policy, rather than separate exclusion lists.
//
//go:embed contracts/cloud-index-path-policy.json
var indexPathPolicyJSON []byte

type cloudIndexPathPolicy struct {
	Version                  int      `json:"version"`
	CaseSensitive            bool     `json:"caseSensitive"`
	ExcludedSegments         []string `json:"excludedSegments"`
	ExcludedBasenames        []string `json:"excludedBasenames"`
	ExcludedBasenamePrefixes []string `json:"excludedBasenamePrefixes"`
	ExcludedBasenameSuffixes []string `json:"excludedBasenameSuffixes"`
	ExcludedExtensions       []string `json:"excludedExtensions"`
}

var indexPathPolicy = func() cloudIndexPathPolicy {
	var policy cloudIndexPathPolicy
	if err := json.Unmarshal(indexPathPolicyJSON, &policy); err != nil {
		panic(err)
	}
	if policy.Version != 1 || policy.CaseSensitive {
		panic("unsupported cloud index path policy")
	}
	return policy
}()

func isCloudIndexPathExcluded(normalized string) bool {
	lower := strings.ToLower(strings.TrimSuffix(normalized, "/"))
	for _, segment := range strings.Split(lower, "/") {
		for _, excluded := range indexPathPolicy.ExcludedSegments {
			if segment == excluded {
				return true
			}
		}
	}
	base := path.Base(lower)
	for _, excluded := range indexPathPolicy.ExcludedBasenames {
		if base == excluded {
			return true
		}
	}
	for _, prefix := range indexPathPolicy.ExcludedBasenamePrefixes {
		if strings.HasPrefix(base, prefix) {
			return true
		}
	}
	for _, suffix := range indexPathPolicy.ExcludedBasenameSuffixes {
		if strings.HasSuffix(base, suffix) {
			return true
		}
	}
	for _, extension := range indexPathPolicy.ExcludedExtensions {
		if path.Ext(base) == extension {
			return true
		}
	}
	return false
}
