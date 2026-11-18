package ssrf

import (
	"context"
	"errors"
	"net"
	"testing"
)

type fakeResolver map[string][]net.IP

func (f fakeResolver) LookupIP(_ context.Context, host string) ([]net.IP, error) {
	ips, ok := f[host]
	if !ok {
		return nil, errors.New("no such host")
	}
	return ips, nil
}

func guardWith(resolver fakeResolver) *Guard { return &Guard{Resolver: resolver} }

func TestIsPublicIP(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"93.184.216.34", true},
		{"8.8.8.8", true},
		{"2606:2800:220:1:248:1893:25c8:1946", true},
		{"10.0.0.5", false},
		{"172.16.4.1", false},
		{"192.168.1.1", false},
		{"127.0.0.1", false},
		{"::1", false},
		{"169.254.169.254", false}, // cloud metadata
		{"100.64.1.1", false},      // CGNAT
		{"0.0.0.0", false},
		{"224.0.0.1", false}, // multicast
		{"fe80::1", false},   // link-local v6
	}
	for _, c := range cases {
		if got := IsPublicIP(net.ParseIP(c.ip)); got != c.want {
			t.Errorf("IsPublicIP(%s) = %v, want %v", c.ip, got, c.want)
		}
	}
}

func TestValidateURLAcceptsPublicHost(t *testing.T) {
	g := guardWith(fakeResolver{"hooks.example.com": {net.ParseIP("93.184.216.34")}})
	if err := g.ValidateURL(context.Background(), "https://hooks.example.com/orders"); err != nil {
		t.Fatalf("public url rejected: %v", err)
	}
}

func TestValidateURLRejectsPrivateResolution(t *testing.T) {
	g := guardWith(fakeResolver{"evil.example.com": {net.ParseIP("10.1.2.3")}})
	if err := g.ValidateURL(context.Background(), "http://evil.example.com/"); err == nil {
		t.Fatal("expected rejection for private ip")
	}
}

func TestValidateURLRejectsMixedResolution(t *testing.T) {
	// One public + one private address: the private one must poison the host.
	g := guardWith(fakeResolver{"mixed.example.com": {net.ParseIP("8.8.8.8"), net.ParseIP("192.168.0.1")}})
	if err := g.ValidateURL(context.Background(), "https://mixed.example.com/"); err == nil {
		t.Fatal("expected rejection for mixed resolution")
	}
}

func TestValidateURLRejectsLiteralPrivateIPs(t *testing.T) {
	g := guardWith(fakeResolver{})
	for _, raw := range []string{
		"http://127.0.0.1/admin",
		"http://169.254.169.254/latest/meta-data/",
		"http://[::1]/",
		"https://192.168.1.1/",
	} {
		if err := g.ValidateURL(context.Background(), raw); err == nil {
			t.Errorf("expected rejection for %s", raw)
		}
	}
}

func TestValidateURLRejectsSchemesAndPorts(t *testing.T) {
	g := guardWith(fakeResolver{"example.com": {net.ParseIP("93.184.216.34")}})
	for _, raw := range []string{
		"ftp://example.com/file",
		"file:///etc/passwd",
		"gopher://example.com/",
		"http://example.com:8080/",
		"https://example.com:22/",
		"http://example.com:0/",
	} {
		if err := g.ValidateURL(context.Background(), raw); err == nil {
			t.Errorf("expected rejection for %s", raw)
		}
	}
	for _, raw := range []string{
		"http://example.com/",
		"http://example.com:80/",
		"https://example.com/",
		"https://example.com:443/path?q=1",
	} {
		if err := g.ValidateURL(context.Background(), raw); err != nil {
			t.Errorf("expected acceptance for %s, got %v", raw, err)
		}
	}
}

func TestValidateURLUnknownHost(t *testing.T) {
	g := guardWith(fakeResolver{})
	if err := g.ValidateURL(context.Background(), "https://missing.example.com/"); err == nil {
		t.Fatal("expected rejection for unresolvable host")
	}
}
