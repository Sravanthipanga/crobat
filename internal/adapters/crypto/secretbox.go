// Package crypto implements the ports.SecretBox with AES-256-GCM.
// Ciphertexts are self-describing: "enc:v1:" + base64(nonce || ciphertext),
// so stored rows can be told apart from plaintext at a glance and the
// format can evolve.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/Sravanthipanga/crobat/internal/domain"
)

// Prefix marks an encrypted value in stored form.
const Prefix = "enc:v1:"

// SecretBox encrypts secret header values at rest.
type SecretBox struct {
	gcm cipher.AEAD
}

// NewSecretBox builds a box from a base64-encoded 32-byte key
// (env CROBAT_SECRET_KEY).
func NewSecretBox(keyBase64 string) (*SecretBox, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(keyBase64))
	if err != nil {
		return nil, fmt.Errorf("secret key is not valid base64: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("secret key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &SecretBox{gcm: gcm}, nil
}

// NewRandomSecretBox builds a box with a fresh random key (tests, local dev).
func NewRandomSecretBox() (*SecretBox, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return NewSecretBox(base64.StdEncoding.EncodeToString(key))
}

// IsEncrypted reports whether a stored value is in encrypted form.
func IsEncrypted(s string) bool { return strings.HasPrefix(s, Prefix) }

// Encrypt encrypts one value. Already-encrypted values pass through, so
// re-saving a task never double-encrypts.
func (b *SecretBox) Encrypt(plaintext string) (string, error) {
	if IsEncrypted(plaintext) {
		return plaintext, nil
	}
	nonce := make([]byte, b.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ct := b.gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return Prefix + base64.StdEncoding.EncodeToString(ct), nil
}

// Decrypt reverses Encrypt. Plaintext (unprefixed) values pass through so
// rows written before encryption was enabled still work.
func (b *SecretBox) Decrypt(ciphertext string) (string, error) {
	if !IsEncrypted(ciphertext) {
		return ciphertext, nil
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(ciphertext, Prefix))
	if err != nil {
		return "", fmt.Errorf("corrupt ciphertext: %w", err)
	}
	ns := b.gcm.NonceSize()
	if len(raw) < ns {
		return "", errors.New("corrupt ciphertext: too short")
	}
	pt, err := b.gcm.Open(nil, raw[:ns], raw[ns:], nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}
	return string(pt), nil
}

// EncryptSecrets returns a copy of spec with every SecretHeaders value
// encrypted — the form that is stored and copied into job rows.
func EncryptSecrets(box *SecretBox, spec domain.RequestSpec) (domain.RequestSpec, error) {
	out := spec.Clone()
	for _, name := range spec.SecretHeaders {
		v, ok := out.Headers[name]
		if !ok {
			continue
		}
		enc, err := box.Encrypt(v)
		if err != nil {
			return domain.RequestSpec{}, err
		}
		out.Headers[name] = enc
	}
	return out, nil
}

// DecryptSecrets returns a copy of spec with secret values decrypted —
// the form used to make the actual HTTP call. Plaintext values (e.g. a
// header listed as secret but stored before encryption) pass through.
func DecryptSecrets(box *SecretBox, spec domain.RequestSpec) (domain.RequestSpec, error) {
	out := spec.Clone()
	for _, name := range spec.SecretHeaders {
		v, ok := out.Headers[name]
		if !ok {
			continue
		}
		dec, err := box.Decrypt(v)
		if err != nil {
			return domain.RequestSpec{}, err
		}
		out.Headers[name] = dec
	}
	return out, nil
}
