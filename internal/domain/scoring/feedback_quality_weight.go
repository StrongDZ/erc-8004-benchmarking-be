package scoring

// QualityWeightConfig holds all tunable hyperparameters for feedback quality weighting.
type QualityWeightConfig struct {
	WiBase float64

	QWeightReasoning  float64
	QWeightAttachment float64
	QWeightBreakdown  float64
	QWeightPayment    float64
	QWeightConfidence float64

	ReasoningLenFull    int
	AttachmentCountFull int
}

// DefaultQualityWeightConfig returns the design-doc recommended values.
func DefaultQualityWeightConfig() QualityWeightConfig {
	return QualityWeightConfig{
		WiBase: 0.7,

		QWeightReasoning:  0.20,
		QWeightAttachment: 0.25,
		QWeightBreakdown:  0.20,
		QWeightPayment:    0.15,
		QWeightConfidence: 0.20,

		ReasoningLenFull:    200,
		AttachmentCountFull: 1,
	}
}

// FeedbackQualityInput holds quality signals extracted from a feedback record.
type FeedbackQualityInput struct {
	ReasoningLen         int
	AttachmentCount      int
	HasRatingBreakdown   bool
	HasProofOfPayment    bool
	ClassifierConfidence float64
}

// ComputeFeedbackQuality returns Q ∈ [0, 1].
// Formula: Q = 0.20·R + 0.25·A + 0.20·B + 0.15·P + 0.20·C
func ComputeFeedbackQuality(cfg QualityWeightConfig, in FeedbackQualityInput) float64 {
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

// ComputeFeedbackQualityWeight returns the feedback's quality weight wi ∈ [WiBase, 1].
// Formula: wi = WiBase + (1 - WiBase)·Q, Q = qualityScore ∈ [0,1].
//
// Sender trust is intentionally NOT a factor: including it here would double-count
// sender trust inside agent reputation (Σ wᵢ·dᵢ·vᵢ).
func ComputeFeedbackQualityWeight(cfg QualityWeightConfig, qualityScore float64) float64 {
	q := clipFloat(qualityScore, 0, 1)
	return cfg.WiBase + (1-cfg.WiBase)*q
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
