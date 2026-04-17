package dto

// misc.go — OASF discovery & chain metadata DTOs.

// OASFFacetNode mirrors agent.OASFFacetNode but JSON-tagged for public API.
type OASFFacetNode struct {
	Key       string `json:"key"`
	Count     int64  `json:"count"`
	Name      string `json:"name"`
	ParentKey string `json:"parentKey,omitempty"`
	Children  []OASFFacetNode `json:"children,omitempty"`
}

// OASFFacetTree is the /oasf/facets response (§4.1).
type OASFFacetTree struct {
	AllSkillsCount  int64           `json:"allSkillsCount"`
	AllDomainsCount int64           `json:"allDomainsCount"`
	SkillNodes      []OASFFacetNode `json:"skillNodes"`
	DomainNodes     []OASFFacetNode `json:"domainNodes"`
}

// ChainInfo is one entry of /chains (§5.1).
type ChainInfo struct {
	ChainID        int64  `json:"chainId"`
	Name           string `json:"name"`
	ShortName      string `json:"shortName"`
	NativeCurrency string `json:"nativeCurrency,omitempty"`
	BlockExplorer  string `json:"blockExplorer,omitempty"`
	AgentCount     int64  `json:"agentCount"`
}

// ContractInfo is one entry of /chains/:id/contracts (§5.2).
type ContractInfo struct {
	Type       string `json:"type"`
	Address    string `json:"address"`
	StartBlock uint64 `json:"startBlock,omitempty"`
}

// IndexerChainStatus is /admin/indexer-status.chains[i] (§6.1).
type IndexerChainStatus struct {
	ChainID            int64   `json:"chainId"`
	LastProcessedBlock uint64  `json:"lastProcessedBlock"`
	CrawlerStatus      string  `json:"crawlerStatus,omitempty"`
	LastError          string  `json:"lastError,omitempty"`
	LastUpdatedAt      string  `json:"lastUpdatedAt,omitempty"`
	ActiveRPC          string  `json:"activeRpc,omitempty"`
}

// WorkerCursor is a (blockNumber, logIndex) cursor as returned by indexer-status.
type WorkerCursor struct {
	BlockNumber uint64 `json:"blockNumber"`
	LogIndex    uint   `json:"logIndex"`
}

// WorkerStatus represents the state of one background worker.
type WorkerStatus struct {
	Running bool          `json:"running"`
	Cursor  *WorkerCursor `json:"cursor,omitempty"`
}

// IndexerStatus is the full /admin/indexer-status response (§6.1).
type IndexerStatus struct {
	Chains       []IndexerChainStatus    `json:"chains"`
	Workers      map[string]WorkerStatus `json:"workers"`
	Events24H    int64                   `json:"events24h"`
	Feedbacks24H int64                   `json:"feedbacks24h"`
}
