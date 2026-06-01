package metrics

// separation.go — Cohen's d effect size between two groups.

import "math"

// CohensD computes the standardized mean difference between groups.
// d = (mean_a - mean_b) / pooled_std
// Returns 0 when either group is empty or pooled std is zero.
func CohensD(a, b []float64) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	da := Describe(a)
	db := Describe(b)
	na, nb := float64(da.N), float64(db.N)
	if na+nb-2 <= 0 {
		return 0
	}
	pooled := math.Sqrt(((na-1)*da.Std*da.Std + (nb-1)*db.Std*db.Std) / (na + nb - 2))
	if pooled == 0 {
		return 0
	}
	return (da.Mean - db.Mean) / pooled
}
