package main

import (
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestAnnotateError(t *testing.T) {
	var err error

	annotateError(&err, "unused")

	if err != nil {
		t.Errorf("annotateError(nil) modified error: %v", err)
	}

	err = os.ErrInvalid

	annotateError(&err, "first=%d, second=%d", 1, 2)

	if !strings.HasPrefix(err.Error(), "first=1, second=2:") {
		t.Errorf("annotateError(ErrInvalid) returned wrong prefix: %v", err)
	}
}

func TestIsNotExist(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil"},
		{
			name: "invalid",
			err:  os.ErrInvalid,
		},
		{
			name: "NoSuchKey",
			err: &types.NoSuchKey{
				Message: aws.String("mykey"),
			},
			want: true,
		},
		{
			name: "API error",
			err: &smithy.GenericAPIError{
				Code:    errorCodeNoSuchKey,
				Message: "mykey",
			},
			want: true,
		},
		{
			name: "unrelated API error",
			err:  &smithy.GenericAPIError{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := isNotExist(tc.err)

			if got != tc.want {
				t.Errorf("isNotExist(%#v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestNewClientFromName(t *testing.T) {
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

			got, err := newClientFromName(cfg, tc.input)

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
