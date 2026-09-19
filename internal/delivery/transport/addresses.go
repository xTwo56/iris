package transport

import "net/netip"

// Be conservative about special-purpose destinations, including globally routed
// special-use exceptions. Based on IANA IPv4/IPv6 Special-Purpose registries:
// https://www.iana.org/assignments/iana-ipv4-special-registry
// https://www.iana.org/assignments/iana-ipv6-special-registry
// Review this static policy when those registries change. IPv6 is restricted to
// ordinary 2000::/3 global space, excluding protocol, documentation and transition
// ranges. IsGlobalUnicast alone incorrectly admits private and special addresses.
var denied = func() []netip.Prefix {
	var result []netip.Prefix
	for _, s := range []string{
		"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
		"192.0.0.0/24", "192.0.2.0/24", "192.31.196.0/24", "192.52.193.0/24", "192.88.99.0/24", "192.168.0.0/16", "192.175.48.0/24",
		"198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
		// Azure's platform virtual IP is special even though outside IANA private space.
		"168.63.129.16/32",
		"2001::/23", "2001:db8::/32", "2002::/16", "2620:4f:8000::/48", "3ffe::/16", "3fff::/20",
	} {
		result = append(result, netip.MustParsePrefix(s))
	}
	return result
}()
var ipv6Global = netip.MustParsePrefix("2000::/3")

func publicIP(ip netip.Addr) bool {
	if !ip.IsValid() || ip.Zone() != "" {
		return false
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.Is6() && !ipv6Global.Contains(ip) {
		return false
	}
	for _, p := range denied {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}
