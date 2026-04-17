package utils

type BlockRange struct {
	From uint64
	To   uint64
}

func BuildForwardRanges(start uint64, end uint64, batchSize uint64) []BlockRange {
	if start > end || batchSize == 0 {
		return nil
	}
	out := make([]BlockRange, 0)
	for from := start; from <= end; {
		to := from + batchSize - 1
		if to > end {
			to = end
		}
		out = append(out, BlockRange{From: from, To: to})
		if to == ^uint64(0) {
			break
		}
		from = to + 1
	}
	return out
}
