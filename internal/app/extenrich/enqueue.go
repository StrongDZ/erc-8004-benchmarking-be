package extenrich

// enqueue.go — non-blocking enqueue of rich/ENS work onto the rate-limited
// worker pools. A full buffer defers to the next backlog sweep.

import (
	"log"

	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// enqueueDownstream pushes w onto explorerJobs and ensJobs when those passes
// are still pending. Sends are non-blocking; a full buffer defers to sweep.
func (a *App) enqueueDownstream(w wallet.WalletDocument) {
	a.tryEnqueueExplorer(w)
	a.tryEnqueueENS(w)
}

func (a *App) tryEnqueueExplorer(w wallet.WalletDocument) {
	if len(a.explorerClients) == 0 {
		return
	}
	if w.External.RichFetched || w.External.ExplorerSkipped {
		return
	}
	if !extscore.ExplorerApplicable(w.ChainID) {
		return
	}
	select {
	case a.explorerJobs <- w:
	default:
		log.Printf("extenrich: explorer queue full, defer %s to sweep", w.ID)
	}
}

func (a *App) tryEnqueueENS(w wallet.WalletDocument) {
	if a.ensClient == nil || w.External.ENSFetched {
		return
	}
	select {
	case a.ensJobs <- w:
	default:
		log.Printf("extenrich: ens queue full, defer %s to sweep", w.ID)
	}
}
