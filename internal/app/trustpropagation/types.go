package trustpropagation

type NodeKind uint8

const (
	NodeKindWallet NodeKind = 0
	NodeKindAgent  NodeKind = 1
)

// GraphNode is a node in the trust graph.
type GraphNode struct {
	ID        string
	Kind      NodeKind
	DirectRep float64 // [0,100] direct-reputation prior (teleport input)
	OwnerID   string  // agent-only: node ID of the owning wallet ("" if none)
	Weight    float64 // agent-only: evidence mass (weightMass) for owner aggregation
}

// GraphEdge is a directed client→agent trust edge (frequency weight).
type GraphEdge struct {
	From   string
	To     string
	Weight float64 // count of valid feedbacks from this client to this agent
}

// GraphData holds the full graph snapshot. agent→owner is derived from GraphNode.OwnerID.
type GraphData struct {
	Nodes []GraphNode
	Edges []GraphEdge
}

// WalletScores is the output of a propagation pass.
type WalletScores struct {
	Rated   map[string]float64 // wallet node ID -> [0,100] (p99-normalized)
	Unrated []string           // wallet node IDs with no trust evidence
}
