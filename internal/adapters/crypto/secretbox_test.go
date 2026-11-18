package crypto

import (
	"strings"
	"testing"

	"github.com/Sravanthipanga/crobat/internal/domain"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	box, err := NewRandomSecretBox()
	if err != nil {
		t.Fatal(err)
	}
	ct, err := box.Encrypt("super-secret-token")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ct, Prefix) {
		t.Fatalf("ciphertext %q missing prefix", ct)
	}
	if strings.Contains(ct, "super-secret-token") {
		t.Fatal("ciphertext leaks plaintext")
	}
	pt, err := box.Decrypt(ct)
	if err != nil {
		t.Fatal(err)
	}
	if pt != "super-secret-token" {
		t.Fatalf("got %q", pt)
	}
}

func TestDecryptWithWrongKeyFails(t *testing.T) {
	a, _ := NewRandomSecretBox()
	b, _ := NewRandomSecretBox()
	ct, err := a.Encrypt("secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Decrypt(ct); err == nil {
		t.Fatal("expected decrypt with wrong key to fail")
	}
}

func TestEncryptIsIdempotentAndDecryptPassesPlaintext(t *testing.T) {
	box, _ := NewRandomSecretBox()
	ct, _ := box.Encrypt("secret")
	again, err := box.Encrypt(ct)
	if err != nil || again != ct {
		t.Fatalf("double encrypt: %q err %v", again, err)
	}
	pt, err := box.Decrypt("not-encrypted")
	if err != nil || pt != "not-encrypted" {
		t.Fatalf("plaintext passthrough: %q err %v", pt, err)
	}
}

func TestNewSecretBoxRejectsBadKeys(t *testing.T) {
	if _, err := NewSecretBox("not-base64!!"); err == nil {
		t.Fatal("expected error for non-base64 key")
	}
	if _, err := NewSecretBox("aGk="); err == nil { // "hi", 2 bytes
		t.Fatal("expected error for short key")
	}
}

func TestEncryptSecretsRoundTrip(t *testing.T) {
	box, _ := NewRandomSecretBox()
	spec := domain.RequestSpec{
		Method: "POST",
		URL:    "https://example.com/hook",
		Headers: map[string]string{
			"Authorization": "Bearer abc123",
			"Content-Type":  "application/json",
		},
		SecretHeaders: []string{"Authorization"},
	}
	stored, err := EncryptSecrets(box, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !IsEncrypted(stored.Headers["Authorization"]) {
		t.Fatal("secret header was not encrypted")
	}
	if stored.Headers["Content-Type"] != "application/json" {
		t.Fatal("non-secret header was modified")
	}
	if spec.Headers["Authorization"] != "Bearer abc123" {
		t.Fatal("EncryptSecrets mutated its input")
	}
	plain, err := DecryptSecrets(box, stored)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Headers["Authorization"] != "Bearer abc123" {
		t.Fatalf("round trip got %q", plain.Headers["Authorization"])
	}
}
