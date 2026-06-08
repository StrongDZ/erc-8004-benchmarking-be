package propagation

// PropagationConfig holds all tunable hyperparameters for the trust graph.
type PropagationConfig struct {
	WiBase float64

	QWeightReasoning   float64
	QWeightAttachment  float64
	QWeightBreakdown   float64
	QWeightPayment     float64
	QWeightConfidence  float64

	ReasoningLenFull    int
	AttachmentCountFull int

	Eta   float64
	Kappa float64
}

// DefaultPropagationConfig returns the design-doc recommended values.
func DefaultPropagationConfig() PropagationConfig {
	return PropagationConfig{
		WiBase: 0.4,

		QWeightReasoning:  0.20,
		QWeightAttachment: 0.25,
		QWeightBreakdown:  0.20,
		QWeightPayment:    0.15,
		QWeightConfidence: 0.20,

		ReasoningLenFull:    200,
		AttachmentCountFull: 3,

		Eta:   0.5,
		Kappa: 2.0,
	}
}
