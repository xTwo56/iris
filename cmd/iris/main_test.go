package main

import (
	"strings"
	"testing"
)

func TestConfig(t *testing.T) {
	for _, token := range []string{"", "  ", "a b"} {
		_, err := loadConfig(func(k string) string {
			if k == "IRIS_MANAGEMENT_TOKEN" {
				return token
			}
			return "db"
		})
		if err == nil {
			t.Fatal("invalid token accepted")
		}
	}
	c, err := loadConfig(func(k string) string {
		switch k {
		case "IRIS_MANAGEMENT_TOKEN":
			return "secret"
		case "IRIS_SECRET_ENCRYPTION_KEY":
			return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
		case "IRIS_DATABASE_URL":
			return "db"
		}
		return ""
	})
	if err != nil || c.address != "127.0.0.1:8080" {
		t.Fatalf("%+v %v", c, err)
	}
	_, err = loadConfig(func(k string) string {
		if k == "IRIS_MANAGEMENT_TOKEN" {
			return "secret"
		}
		return ""
	})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("bad configuration error")
	}
}

func TestEncryptionConfig(t *testing.T) {
	for _, key := range []string{"", "bad", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="} {
		_, err := loadConfig(func(k string) string {
			switch k {
			case "IRIS_MANAGEMENT_TOKEN":
				return "bearer"
			case "IRIS_DATABASE_URL":
				return "db"
			case "IRIS_SECRET_ENCRYPTION_KEY":
				return key
			}
			return ""
		})
		if err == nil {
			t.Fatal("bad encryption key accepted")
		}
	}
}
