package nds_test

import (
	"appengine"
	"appengine/aetest"
	"appengine/datastore"
	"github.com/qedus/nds"
	"testing"
)

func TestRunInTransaction(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	entity := &testEntity{42}
	key := datastore.NewKey(c, "Entity", "", 3, nil)

	if _, err := datastore.Put(c, key, entity); err != nil {
		t.Fatal(err)
	}

	cc := nds.NewContext(c)
	err = nds.RunInTransaction(cc, func(tc appengine.Context) error {
		entity = &testEntity{}
		if err := nds.Get(tc, key, entity); err != nil {
			t.Fatal(err)
		}
		if entity.Val != 42 {
			t.Fatalf("entity.Val != 42: %d", entity.Val)
		}
		entity.Val = 43
		if putKey, err := nds.Put(tc, key, entity); err != nil {
			t.Fatal(err)
		} else if !putKey.Equal(key) {
			t.Fatal("keys not equal")
		}
		entity = &testEntity{}
		if err := nds.Get(tc, key, entity); err != nil {
			t.Fatal(err)
		}
		if entity.Val != 43 {
			t.Fatalf("entity.Val != 43: %d", entity.Val)
		}
		return nil

	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	entity = &testEntity{}
	if err := datastore.Get(c, key, entity); err != nil {
		t.Fatal(err)
	}
	if entity.Val != 43 {
		t.Fatalf("incorrect entity value: %d", entity.Val)
	}
}
