package lib

import (
	"reflect"
	"strings"
	"testing"
)

func TestSplitSelectList(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"id", []string{"id"}},
		{"id, lower(email) AS email_lc", []string{"id", "lower(email) AS email_lc"}},
		{"  id ,  email  ", []string{"id", "email"}},
		{"to_char(created_at, 'YYYY-MM-DD') AS created_on", []string{"to_char(created_at, 'YYYY-MM-DD') AS created_on"}},
		{"concat(a, b) AS ab", []string{"concat(a, b) AS ab"}},
		{"payload->>'x,y' AS v", []string{"payload->>'x,y' AS v"}},
		{`'a,b' AS s, "c,d" AS q`, []string{`'a,b' AS s`, `"c,d" AS q`}},
		{`'it''s, fine' AS s`, []string{`'it''s, fine' AS s`}},
		{`"a""b,c" AS q`, []string{`"a""b,c" AS q`}},
		{"array[1,2][1] AS first, id", []string{"array[1,2][1] AS first", "id"}},
		{"$tag$a,b$tag$ AS d, id", []string{"$tag$a,b$tag$ AS d", "id"}},
		{"$$a,b$$ AS d", []string{"$$a,b$$ AS d"}},
		{"$1 + price AS p", []string{"$1 + price AS p"}},
		{"coalesce(x, (SELECT y FROM t WHERE z = 1)), id", []string{"coalesce(x, (SELECT y FROM t WHERE z = 1))", "id"}},
	}
	for _, tc := range cases {
		got, err := splitSelectList(tc.in)
		if err != nil {
			t.Errorf("splitSelectList(%q): %v", tc.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("splitSelectList(%q) = %#v, want %#v", tc.in, got, tc.want)
		}
	}
}

func TestSplitSelectListErrors(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", "empty"},
		{"   ", "empty"},
		{"id,", "empty"},
		{",id", "empty"},
		{"id,,email", "empty"},
		{"lower(email", "unbalanced '('"},
		{"lower(email))", "unbalanced ')'"},
		{"'oops", "unterminated string quote"},
		{`"oops`, "unterminated identifier quote"},
		{"$tag$oops", "unterminated dollar-quoted string"},
	}
	for _, tc := range cases {
		_, err := splitSelectList(tc.in)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("splitSelectList(%q) error = %v, want one containing %q", tc.in, err, tc.want)
		}
	}
}
