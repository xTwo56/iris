package signing_test

import (
	"fmt"
	"github.com/xTwo56/iris/internal/webhook/signing"
	"time"
)

// A receiver reads the bounded raw request body and the four headers, then calls
// Verify (which uses hmac.Equal). Reject duplicate header values before this call.
// After verification, atomically deduplicate the decoded delivery ID before work.
func ExampleVerify() {
	secret := make([]byte, 32) // Example only: use the decoded creation-response secret.
	body := []byte(`{"event":"created"}`)
	now := time.Unix(1700000000, 0)
	headers, _ := signing.Sign(secret, body, "event-1", "delivery-1", now)
	err := signing.Verify(secret, body, headers, now, 5*time.Minute)
	fmt.Println(err == nil)
	// Output: true
}
