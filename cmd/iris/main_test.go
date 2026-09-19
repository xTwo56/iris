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
