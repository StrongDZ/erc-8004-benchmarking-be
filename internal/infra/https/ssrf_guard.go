package https

// ssrf_guard.go — Dial-time IP allow-listing for off-chain URI fetches.
//
// agentURI/feedbackURI values come from on-chain registrations and are fully
// attacker-controlled. Without this guard, a malicious agent could point its
// URI at an internal service (e.g. http://169.254.169.254/, http://localhost:6379)
// and use this server as an SSRF proxy. The check runs on the resolved IP at
// dial time (not on the hostname before resolution) so it also covers DNS
// rebinding, where a hostname's DNS answer changes between check and connect.

import (
	"context"
	"fmt"
	"net"
)

// safeDialContext resolves addr's host, rejects any resolved IP that falls in
// a private, loopback, link-local, unspecified, or multicast range, and only
// dials addresses that pass the check.
func safeDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("https fetch: invalid address %q: %w", addr, err)
	}

	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("https fetch: resolve host %q: %w", host, err)
	}

	var dialer net.Dialer
	var lastErr error
	for _, ip := range ips {
		if isBlockedIP(ip) {
			lastErr = fmt.Errorf("https fetch: refusing to connect to disallowed address %s (host=%q)", ip, host)
			continue
		}
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("https fetch: no addresses resolved for host %q", host)
	}
	return nil, lastErr
}

func isBlockedIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast()
}
