package main

import (
	"runtime/debug"
	"testing"
)

func TestResolveVersion(t *testing.T) {
	buildInfo := func(mainVersion string, settings map[string]string) *debug.BuildInfo {
		bi := &debug.BuildInfo{}
		bi.Main.Version = mainVersion
		for k, v := range settings {
			bi.Settings = append(bi.Settings, debug.BuildSetting{Key: k, Value: v})
		}
		return bi
	}

	for _, tt := range []struct {
		name     string
		injected string
		bi       *debug.BuildInfo
		want     string
	}{
		{
			name:     "InjectedVersionWins",
			injected: "v0.5.13-6-g2b34013-dirty",
			bi:       buildInfo("v0.5.14", nil),
			want:     "v0.5.13-6-g2b34013-dirty",
		},
		{
			name:     "ModuleVersionFromBuildInfo",
			injected: defaultVersion,
			bi:       buildInfo("v0.5.13+dirty", nil),
			want:     "v0.5.13+dirty",
		},
		{
			name:     "EmptyInjectedFallsBackToBuildInfo",
			injected: "",
			bi:       buildInfo("v0.5.13", nil),
			want:     "v0.5.13",
		},
		{
			name:     "DevelModuleVersionFallsBackToRevision",
			injected: defaultVersion,
			bi: buildInfo("(devel)", map[string]string{
				"vcs.revision": "2b340139876543210fedcba9876543210fedcba9",
				"vcs.modified": "false",
			}),
			want: "(development build 2b3401398765)",
		},
		{
			name:     "DirtyRevisionMarked",
			injected: defaultVersion,
			bi: buildInfo("(devel)", map[string]string{
				"vcs.revision": "2b340139876543210fedcba9876543210fedcba9",
				"vcs.modified": "true",
			}),
			want: "(development build 2b3401398765-dirty)",
		},
		{
			name:     "ShortRevisionNotTruncated",
			injected: defaultVersion,
			bi: buildInfo("(devel)", map[string]string{
				"vcs.revision": "2b34013",
				"vcs.modified": "false",
			}),
			want: "(development build 2b34013)",
		},
		{
			name:     "NoBuildInfo",
			injected: defaultVersion,
			bi:       nil,
			want:     defaultVersion,
		},
		{
			name:     "NoVersionInfoAtAll",
			injected: defaultVersion,
			bi:       buildInfo("(devel)", nil),
			want:     defaultVersion,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveVersion(tt.injected, tt.bi); got != tt.want {
				t.Fatalf("resolveVersion()=%q, want %q", got, tt.want)
			}
		})
	}
}
