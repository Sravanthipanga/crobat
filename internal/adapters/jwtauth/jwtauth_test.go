package jwtauth

import "testing"

func TestSignVerifyRoundTrip(t *testing.T) {
	v := NewVerifier("secret-1")
	tok, err := v.Sign("tenant-9")
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := v.Verify(tok)
	if err != nil || tenant != "tenant-9" {
		t.Fatalf("tenant=%q err=%v", tenant, err)
	}
}

func TestVerifyRejectsWrongSecretAndGarbage(t *testing.T) {
	signer := NewVerifier("secret-1")
	tok, err := signer.Sign("tenant-9")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewVerifier("secret-2").Verify(tok); err == nil {
		t.Fatal("token verified under the wrong secret")
	}
	if _, err := signer.Verify("not-a-token"); err == nil {
		t.Fatal("garbage token verified")
	}
	if _, err := NewVerifier("").Verify(tok); err == nil {
		t.Fatal("token verified with no secret configured")
	}
}
