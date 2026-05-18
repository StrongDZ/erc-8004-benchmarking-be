package propagation

// QualityInput holds quality signals extracted from a feedback record.
type QualityInput struct {
	ReasoningLen         int
	AttachmentCount      int
	HasRatingBreakdown   bool
	HasProofOfPayment    bool
	ClassifierConfidence float64
}

// ComputeQualityScore returns Q ∈ [0, 1].
// Formula: Q = 0.20·R + 0.25·A + 0.20·B + 0.15·P + 0.20·C
func ComputeQualityScore(cfg PropagationConfig, in QualityInput) float64 {
	R := minFloat(float64(in.ReasoningLen)/float64(cfg.ReasoningLenFull), 1.0)
	if R < 0 {
		R = 0
	}
	A := minFloat(float64(in.AttachmentCount)/float64(cfg.AttachmentCountFull), 1.0)
	if A < 0 {
		A = 0
	}
	B := boolToFloat(in.HasRatingBreakdown)
	P := boolToFloat(in.HasProofOfPayment)
	C := clipFloat(in.ClassifierConfidence, 0, 1)

	q := cfg.QWeightReasoning*R +
		cfg.QWeightAttachment*A +
		cfg.QWeightBreakdown*B +
		cfg.QWeightPayment*P +
		cfg.QWeightConfidence*C

	return clipFloat(q, 0, 1)
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func clipFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func boolToFloat(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
