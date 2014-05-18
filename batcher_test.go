package nds

import (
	"appengine/aetest"
	"appengine/datastore"
	"fmt"
	"testing"
)

var printMulti = func(vals []interface{}) error {
	for _, val := range vals {
		fmt.Println(val)
	}
	return nil
}

func TestBatcher(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const loops = int64(50)
	type Entity struct {
		Val int64
	}

	for i := int64(1); i < loops; i++ {
		if i%2 == 0 {
			key := datastore.NewKey(c, "Test", "", i, nil)
			if _, err := datastore.Put(c, key, &Entity{i}); err != nil {
				t.Fatal(err)
			}
		}
	}

	b := newBatcher(c)
	for i := int64(1); i < loops; i++ {
		key := datastore.NewKey(c, "Test", "", i, nil)
		err := b.DatastoreGet(key, datastore.PropertyList{})

		if i%2 == 0 {
			if err != nil {
				t.Fatal("expected nil error")
			}
		} else {
			if err != datastore.ErrNoSuchEntity {
				t.Fatal("expected datastore.ErrNoSuchEntity")
			}
		}
	}
	b.Close()
}
