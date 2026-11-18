// Package ssrf guards executor HTTP calls against server-side request
// forgery. A tenant can ask Crobat to call any URL, so before (and while)
// dialing we enforce:
//
//   - scheme is http or https
//   - port is 80 or 443 (explicit or implied by the scheme)
//   - the host resolves only to public IPs — no private, loopback,
//     link-local, multicast or unspecified addresses
//   - redirects are never followed (a public URL must not bounce the
//     executor at an internal one)
//
// The dialer re-resolves and re-checks at connection time and dials the
// validated IP directly, which closes the DNS-rebinding gap between a
// check and a connect.
package ssrf

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Resolver abstracts DNS lookups so tests can run without a network.
type Resolver interface {
	LookupIP(ctx context.Context, host string) ([]net.IP, error)
}

type netResolver struct{}

func (netResolver) LookupIP(ctx context.Context, host string) ([]net.IP, error) {
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

// Guard validates URLs and dials safely.
type Guard struct {
	Resolver Resolver
	// AllowPrivate permits non-public IPs. It exists ONLY so tests can
	// point the caller at httptest servers; production code never sets it.
	AllowPrivate bool
}

// New returns a Guard using the system resolver.
func New() *Guard { return &Guard{Resolver: netResolver{}} }

func (g *Guard) resolver() Resolver {
	if g.Resolver != nil {
		return g.Resolver
	}
	return netResolver{}
}

// IsPublicIP reports whether ip is a routable public address.
func IsPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	// 100.64.0.0/10 (CGNAT shared space) is covered by IsPrivate in modern
	// Go, but check the v4 ranges explicitly for clarity/safety.
	if v4 := ip.To4(); v4 != nil {
		switch {
		case v4[0] == 0: // 0.0.0.0/8 "this host on this network"
			return false
		case v4[0] == 100 && v4[1]&0xC0 == 0x40: // 100.64.0.0/10
			return false
		case v4[0] == 192 && v4[1] == 0 && v4[2] == 0: // 192.0.0.0/24
			return false
		case v4[0] == 198 && (v4[1] == 18 || v4[1] == 19): // 198.18.0.0/15 benchmarking
			return false
		case v4[0] >= 240: // 240.0.0.0/4 reserved
			return false
		}
	}
	return true
}

// ValidateURL checks scheme, port and resolved IPs for a URL. Literal IP
// hosts are checked directly without DNS.
func (g *Guard) ValidateURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("ssrf: unparsable url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("ssrf: scheme %q not allowed", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("ssrf: url has no host")
	}
	if port := u.Port(); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil || (p != 80 && p != 443) {
			return fmt.Errorf("ssrf: port %q not allowed (80/443 only)", port)
		}
	}
	_, err = g.resolveChecked(ctx, host)
	return err
}

// resolveChecked returns the host's IPs, rejecting the host if ANY of them
// is non-public (an attacker-controlled name must not mix one public and
// one private address).
func (g *Guard) resolveChecked(ctx context.Context, host string) ([]net.IP, error) {
	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		var err error
		ips, err = g.resolver().LookupIP(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("ssrf: resolve %s: %w", host, err)
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("ssrf: host %s resolved to no addresses", host)
	}
	if !g.AllowPrivate {
		for _, ip := range ips {
			if !IsPublicIP(ip) {
				return nil, fmt.Errorf("ssrf: host %s resolves to non-public ip %s", host, ip)
			}
		}
	}
	return ips, nil
}

// DialContext is an http.Transport DialContext: it resolves the host,
// re-validates every IP, and dials a validated IP directly so the address
// that was checked is the address that is connected to.
func (g *Guard) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if port != "80" && port != "443" {
		return nil, fmt.Errorf("ssrf: port %s not allowed (80/443 only)", port)
	}
	ips, err := g.resolveChecked(ctx, host)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{Timeout: 10 * time.Second}
	var lastErr error
	for _, ip := range ips {
		conn, err := d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("ssrf: dial %s: %w", addr, lastErr)
}

// HTTPClient returns an http.Client whose transport dials through the
// guard and which never follows redirects.
func (g *Guard) HTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           g.DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			DisableKeepAlives:     false,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Return the redirect response itself instead of following it.
			return http.ErrUseLastResponse
		},
	}
}

// HostOf is a small helper for error messages and tests.
func HostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}
