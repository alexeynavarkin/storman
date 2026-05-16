// Package storagetest holds test-only helpers shared across the storage
// integration tests: a fixture builder, a consistency checker that walks the
// DB and disk and asserts the durability invariants of `storage.FileSystem`,
// a fault-injecting FileBackend wrapper, and a transactional hook hook used
// by recovery tests to simulate crashes between outbox stages.
//
// The package is *_test* in spirit but lives outside _test.go files so it can
// be imported by multiple storage test packages.
package storagetest
