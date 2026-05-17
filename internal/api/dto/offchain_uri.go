package dto

// OffchainURIDataView is the public shape for GET /offchain-by-uri (offchain_data cache).
type OffchainURIDataView struct {
	Found        bool   `json:"found"`
	Status       int    `json:"status,omitempty"`
	SourceType   string `json:"sourceType,omitempty"`
	EventType    string `json:"eventType,omitempty"`
	ContractType string `json:"contractType,omitempty"`
	FetchError   string `json:"fetchError,omitempty"`
	ContentSize  int    `json:"contentSize,omitempty"`
	// Parsed is the JSON-decoded body when status indicates valid JSON (root object or array).
	Parsed any `json:"parsed,omitempty"`
	// RawPreview is a truncated raw body when fetch succeeded but content was not valid JSON.
	RawPreview string `json:"rawPreview,omitempty"`
}
