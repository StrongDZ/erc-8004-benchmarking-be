package trustpropagation

// NodeKind distinguishes wallet and agent nodes.
type NodeKind uint8

const (
	NodeKindWallet NodeKind = 0
	NodeKindAgent  NodeKind = 1
)

// GraphNode is a node in the trust graph.
type GraphNode struct {
	ID             string
	Kind           NodeKind
	TrustScore     float64 // current trust score [0,100]
	CompositeScore float64 // agent-only: for seed vector
}

// GraphEdge is a directed weighted edge.
type GraphEdge struct {
	From   string
	To     string
	Weight float64
}

// GraphData holds the full graph snapshot.
type GraphData struct {
	Nodes []GraphNode
	Edges []GraphEdge
}
