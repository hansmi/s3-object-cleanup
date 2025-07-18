package main

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestNewBucketFromName(t *testing.T) {
	for _, tc := range []struct {
		name         string
		input        string
		wantErr      error
		wantEndpoint string
		wantBucket   string
		wantPrefix   string
	}{
		{
			name:    "empty",
			wantErr: os.ErrInvalid,
		},
		{
			name:         "url",
			input:        "https://localhost/bucket",
			wantBucket:   "bucket",
			wantEndpoint: "https://localhost",
		},
		{
			name:         "url ending with slash",
			input:        "https://localhost/bucket/",
			wantBucket:   "bucket",
			wantEndpoint: "https://localhost",
		},
		{
			name:         "url with prefix",
			input:        "https://localhost:1234/abcdef/locks/",
			wantBucket:   "abcdef",
			wantEndpoint: "https://localhost:1234",
			wantPrefix:   "locks/",
		},
		{
			name:       "non-url",
			input:      "hello-world",
			wantBucket: "hello-world",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg aws.Config

			got, err := newBucketFromName(cfg, tc.input)

			if diff := cmp.Diff(tc.wantErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Error diff (-want +got):\n%s", diff)
			}

			if err == nil {
				opts := got.client.Options()

				if diff := cmp.Diff(tc.wantEndpoint, aws.ToString(opts.BaseEndpoint)); diff != "" {
					t.Errorf("Endpoint diff (-want +got):\n%s", diff)
				}

				if diff := cmp.Diff(tc.wantBucket, got.name); diff != "" {
					t.Errorf("Bucket diff (-want +got):\n%s", diff)
				}

				if diff := cmp.Diff(tc.wantPrefix, got.prefix); diff != "" {
					t.Errorf("Prefix diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}
