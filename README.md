# Remove non-current object versions from S3 buckets

[![Latest release](https://img.shields.io/github/v/release/hansmi/s3-object-cleanup)][releases]
[![CI workflow](https://github.com/hansmi/s3-object-cleanup/actions/workflows/ci.yaml/badge.svg)](https://github.com/hansmi/s3-object-cleanup/actions/workflows/ci.yaml)
[![Go reference](https://pkg.go.dev/badge/github.com/hansmi/s3-object-cleanup.svg)](https://pkg.go.dev/github.com/hansmi/s3-object-cleanup)

Many cloud storage providers offer services competitive with [AWS
S3](https://aws.amazon.com/s3/). Not all of them support the complete set of
object lifecycle rules, e.g. by not implementing the
`NoncurrentVersionExpiration` functionality. This program provides a solution
by enabling the deletion of non-current object versions after a defined period.

[releases]: https://github.com/hansmi/s3-object-cleanup/releases/latest

<!-- vim: set sw=2 sts=2 et : -->
