package job

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

type pair struct{ key, value string }

func runMap(t *testing.T, j *Job, name, contents string) []pair {
	t.Helper()
	var got []pair
	err := j.Mapper.Map(name, []byte(contents), func(k, v string) { got = append(got, pair{k, v}) })
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	return got
}

func mustNew(t *testing.T, name string, params map[string]string) *Job {
	t.Helper()
	j, err := New(name, params)
	if err != nil {
		t.Fatalf("New(%q): %v", name, err)
	}
	return j
}

func words(text string) []string {
	var got []string
	EachWord([]byte(text), func(w string) { got = append(got, w) })
	return got
}

func TestEachWord(t *testing.T) {
	for text, want := range map[string][]string{
		"It's the END, isn't it?  Über-cool 42 times\tnaïve\n": {"it", "s", "the", "end", "isn", "t", "it", "über", "cool", "times", "naïve"},
		"":              {},
		"  ...  123 ":   {},
		"word":          {"word"},
		"ÉCOLE\xffcafé": {"école", "café"},
	} {
		if got := words(text); !slices.Equal(got, want) {
			t.Errorf("EachWord(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestWordCount(t *testing.T) {
	j := mustNew(t, "wordcount", nil)
	got := runMap(t, j, "a.txt", "the cat and The hat")
	if len(got) != 5 || got[0] != (pair{"the", "1"}) || got[3] != (pair{"the", "1"}) {
		t.Errorf("Map = %v", got)
	}
	sum, err := j.Reducer.Reduce("the", []string{"1", "3", "1"})
	if err != nil || sum != "5" {
		t.Errorf("Reduce = %q, %v; want 5", sum, err)
	}
	// The combiner output feeds the reducer, so they must compose.
	partial, _ := j.Combiner.Reduce("the", []string{"1", "1"})
	if total, _ := j.Reducer.Reduce("the", []string{partial, "1"}); total != "3" {
		t.Errorf("reduce(combine) = %q, want 3", total)
	}
	if _, err := j.Reducer.Reduce("x", []string{"one"}); err == nil {
		t.Error("Reduce accepted a non-numeric count")
	}
}

func TestIndex(t *testing.T) {
	j := mustNew(t, "index", nil)
	got := runMap(t, j, "doc.txt", "to be or not to be")
	want := []pair{{"to", "doc.txt"}, {"be", "doc.txt"}, {"or", "doc.txt"}, {"not", "doc.txt"}}
	if !slices.Equal(got, want) {
		t.Errorf("Map = %v, want each word once: %v", got, want)
	}
	v, err := j.Reducer.Reduce("be", []string{"c.txt", "a.txt", "c.txt", "b.txt"})
	if err != nil || v != "3 a.txt,b.txt,c.txt" {
		t.Errorf("Reduce = %q, %v", v, err)
	}
}

func TestGrep(t *testing.T) {
	j := mustNew(t, "grep", map[string]string{"pattern": `(?i)whale`})
	got := runMap(t, j, "moby.txt", "Call me Ishmael.\r\nThe WHALE!\nno match\nwhales again\n")
	want := []pair{{"moby.txt:2", "The WHALE!"}, {"moby.txt:4", "whales again"}}
	if !slices.Equal(got, want) {
		t.Errorf("Map = %v, want %v", got, want)
	}
	if got := runMap(t, mustNew(t, "grep", map[string]string{"pattern": "^$"}), "e.txt", ""); len(got) != 0 {
		t.Errorf("empty file matched %v", got)
	}
	if got := runMap(t, mustNew(t, "grep", map[string]string{"pattern": "^$"}), "e.txt", "a\n\nb\n"); !slices.Equal(got, []pair{{"e.txt:2", ""}}) {
		t.Errorf("blank-line match = %v, want only line 2", got)
	}
	if v, _ := j.Reducer.Reduce("moby.txt:2", []string{"The WHALE!"}); v != "The WHALE!" {
		t.Errorf("Reduce = %q", v)
	}
}

func TestNewValidatesParams(t *testing.T) {
	for _, tc := range []struct {
		name   string
		job    string
		params map[string]string
		want   string
	}{
		{"unknown job", "nope", nil, "unknown job"},
		{"unknown param", "wordcount", map[string]string{"pattern": "x"}, "does not take parameter"},
		{"missing pattern", "grep", nil, "missing required parameter"},
		{"bad pattern", "grep", map[string]string{"pattern": "("}, "bad pattern"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.job, tc.params)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("New error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestRegistry(t *testing.T) {
	var names []string
	for _, d := range List() {
		names = append(names, d.Name)
	}
	if !slices.IsSorted(names) || !slices.Contains(names, "wordcount") || !slices.Contains(names, "index") || !slices.Contains(names, "grep") {
		t.Errorf("List names = %v", names)
	}
	d, ok := Lookup("grep")
	if !ok || !slices.Equal(slices.Sorted(maps.Keys(d.Params)), []string{"pattern"}) {
		t.Errorf("Lookup(grep) = %+v, %v", d, ok)
	}
	defer func() {
		if recover() == nil {
			t.Error("registering a duplicate name did not panic")
		}
	}()
	Register(Definition{Name: "wordcount", New: func(map[string]string) (*Job, error) { return nil, nil }})
}
