// # Encryption
//
// Miot uses 128-bit AES-CBC encryption.
// The key is the MD5 sum of the token.
// The IV is the MD5 sum of the key and the token.
//
// Block chaining is only within a single packet;
// it does not carry over to other packets.
package wire

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

// A Token is in-memory state needed to encrypt/decrypt a packet.
// It only exports minimal functionality necessary to do its job.
type Token struct {
	block cipher.Block
	token [TokenLen]byte
	key   [md5.Size]byte
	iv    [md5.Size]byte
}

func initToken(dst *Token, token [TokenLen]byte) error {
	var ivSrc bytes.Buffer
	key := md5.Sum(token[:])

	ivSrc.Write(key[:])
	ivSrc.Write(token[:])
	iv := md5.Sum(ivSrc.Bytes())

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return errors.Join(ErrCrypto, err)
	}

	dst.block = block
	dst.iv = iv
	dst.key = key
	dst.token = token

	return nil
}

// NewToken initializes a token's in-memory state
// from its raw byte array form by:
//
// key = md5(token)
//
// iv = md5(concat(key,token))
//
// block = aes(key)
func NewToken(token [TokenLen]byte) (*Token, error) {
	res := new(Token)
	if err := initToken(res, token); err != nil {
		return nil, err
	}
	return res, nil
}

// MarshalText marshals a token into a
// hexadecimal string.
func (tk *Token) MarshalText() ([]byte, error) {
	var buf [TokenLen * 2]byte
	hex.Encode(buf[:], tk.token[:])
	return buf[:], nil
}

// UnmarshalText takes a hexadecimal string then initializes a token.
// The string must not contain a leading "0x".
//
// See [NewToken] for initialization logic.
func (tk *Token) UnmarshalText(text []byte) error {
	// decode hex to bytes, len 16
	var token [TokenLen]byte
	n, err := hex.Decode(token[:], text)
	if err != nil {
		return err
	}
	if n != TokenLen {
		return fmt.Errorf("%w: %v", ErrTokenLen, n)
	}

	return initToken(tk, token)
}

func bw(dst *bytes.Buffer, src []byte, expectedN int) error {
	n, _ := dst.Write(src)
	if n != expectedN {
		return fmt.Errorf("expected to write %v bytes, wrote %v instead", expectedN, n)
	}
	return nil
}

// Marshal transforms MiotPacket into its wire format,
// encrypting it using Token.
func (tk *Token) Marshal(msg *MiotPacket) ([]byte, error) {
	var buf bytes.Buffer
	// [0:2] magic
	if err := bw(&buf, magicRef, 2); err != nil {
		return nil, err
	}

	// [2:4] len
	lenCtPayload := getPaddedSize(msg.Payload, PadBlockSize)
	lenPkt := uint16(LenHeader + lenCtPayload)
	binary.Write(&buf, binary.BigEndian, lenPkt)

	// [4:8] unknown
	buf.Write([]byte{0, 0, 0, 0})

	// [8:12] deviceId
	binary.Write(&buf, binary.BigEndian, msg.DeviceID)

	// [12:16] timestamp
	binary.Write(&buf, binary.BigEndian, msg.Timestamp)

	// [16:32] checksum
	// need padded and encrypted payload
	// pad
	ctPayload := make([]byte, lenCtPayload)
	paddedPayload, err := padBytes(msg.Payload, PadBlockSize)
	if err != nil {
		return nil, err
	}
	// encrypt
	enc := cipher.NewCBCEncrypter(tk.block, tk.iv[:])
	enc.CryptBlocks(ctPayload, paddedPayload)
	tmpBuf := buf.Bytes()
	// hash
	hasher := md5.New()
	hasher.Write(tmpBuf[0:16])
	hasher.Write(tk.token[:])
	hasher.Write(ctPayload)
	hash := hasher.Sum(nil)
	buf.Write(hash)

	// [32:] payload
	buf.Write(ctPayload)

	return buf.Bytes(), nil
}

func (tk *Token) unmarshalNoPayload(packet []byte) (*MiotPacket, error) {
	// [8:12] deviceId
	var deviceId uint32
	pDidSlice := packet[8:12]
	_, err := binary.Decode(pDidSlice, binary.BigEndian, &deviceId)
	if err != nil {
		return nil, fmt.Errorf("%w did: %w", ErrDecode, err)
	}

	// [12:16] timestamp
	var timestamp Timestamp
	pTimestampSlice := packet[12:16]
	_, err = binary.Decode(pTimestampSlice, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, fmt.Errorf("%w timestamp: %w", ErrDecode, err)
	}

	// [16:32] checksum
	cksm := packet[16:32]
	// special case: cksm == 0xffff..ffff
	if bytes.Count(cksm, []byte{0xff}) == 16 {
		return &MiotPacket{
			DeviceID:  DeviceID(deviceId),
			Timestamp: timestamp,
			Payload:   []byte{},
		}, nil
	}

	// verify hash
	hasher := md5.New()
	hasher.Write(packet[0:16])
	hasher.Write(tk.token[:])
	hash := hasher.Sum(nil)
	if !bytes.Equal(cksm, hash) {
		return nil, fmt.Errorf(
			"%w: expected %#x, found %#x",
			ErrHashMismatch, cksm, hash,
		)
	}
	return &MiotPacket{
		DeviceID:  DeviceID(deviceId),
		Timestamp: timestamp,
		Payload:   []byte{},
	}, nil
}

// Unmarshal transforms a wire-format packet into MiotPacket,
// decrypting it using Token.
func (tk *Token) Unmarshal(packet []byte) (*MiotPacket, error) {
	_, err := atLeastHeader(packet)
	if err != nil {
		return nil, err
	}

	// [0:2] magic
	magicSlice := packet[0:2]
	if !bytes.Equal(magicRef, magicSlice) {
		return nil, ErrMagic
	}

	// [2:4] len
	var pLen uint16
	pLenSlice := packet[2:4]
	_, err = binary.Decode(pLenSlice, binary.BigEndian, &pLen)
	if err != nil {
		return nil, err
	}
	if pLen < LenHeader {
		return nil, fmt.Errorf("invalid len %v", pLen)
	}
	if pLen == LenHeader {
		// no payload, must handle separately
		return tk.unmarshalNoPayload(packet)
	}
	expectedLenPayload := int(pLen - LenHeader)

	// [4:8] unknown

	// [8:12] deviceId
	var deviceId uint32
	pDidSlice := packet[8:12]
	_, err = binary.Decode(pDidSlice, binary.BigEndian, &deviceId)
	if err != nil {
		return nil, err
	}

	// [12:16] timestamp
	var timestamp Timestamp
	pTimestampSlice := packet[12:16]
	_, err = binary.Decode(pTimestampSlice, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	// [16:32] checksum
	cksm := packet[16:32]

	// [32:] payload
	ctPayload := packet[32:]
	lenCtPayload := len(ctPayload)
	if lenCtPayload != expectedLenPayload {
		return nil, fmt.Errorf("payload len mismatch: found %v, expected %v", lenCtPayload, expectedLenPayload)
	}

	// verify hash
	hasher := md5.New()
	hasher.Write(packet[0:16])
	hasher.Write(tk.token[:])
	hasher.Write(ctPayload)
	hash := hasher.Sum(nil)
	if !bytes.Equal(hash, cksm) {
		return nil, fmt.Errorf("hash mismatch: expected %x, found %x", cksm, hash)
	}

	// payload is still padded and encrypted
	// decrypt
	dec := cipher.NewCBCDecrypter(tk.block, tk.iv[:])
	paddedPayload := make([]byte, lenCtPayload)
	dec.CryptBlocks(paddedPayload, ctPayload)
	// unpad
	payload, err := unpadBytes(paddedPayload, PadBlockSize)
	if err != nil {
		return nil, err
	}

	return &MiotPacket{
		DeviceID:  DeviceID(deviceId),
		Timestamp: timestamp,
		Payload:   payload,
	}, nil
}
