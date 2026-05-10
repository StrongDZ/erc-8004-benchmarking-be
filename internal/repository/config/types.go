package config

// types.go — Types for config/contract registry documents.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// ConfigEntry is a generic key-value config document. Convention: _id == key.
type ConfigEntry struct {
	ID       string         `bson:"_id"`
	Key      string         `bson:"key"`
	Metadata map[string]any `bson:"metadata,omitempty"`
}

// ConfigRepository wraps the config collection.
type ConfigRepository struct {
	mongorepo.MongoRepoImpl[ConfigEntry]
}
