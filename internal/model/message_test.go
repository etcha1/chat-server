package model

import (
	"reflect"
	"testing"
)

func TestMessageTypeIsNotMappedToDatabase(t *testing.T) {
	field, ok := reflect.TypeOf(Message{}).FieldByName("Type")
	if !ok {
		t.Fatal("Type field not found")
	}

	if got := field.Tag.Get("db"); got != "-" {
		t.Fatalf("Type field db tag = %q, want %q", got, "-")
	}
}
