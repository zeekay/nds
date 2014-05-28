package nds

import (
	"appengine/aetest"
	"appengine/datastore"
	"testing"
)

func TestGetBatcher(t *testing.T) {
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

	b := newBatcher(c, dsGetExecutor)

	for i := int64(1); i < loops; i++ {
		key := datastore.NewKey(c, "Test", "", i, nil)
		entity := &Entity{}
		item := []interface{}{key, entity}
		err := b.Add(item)

		if i%2 == 0 {
			if err != nil {
				t.Fatal("expected nil error")
			}
			if entity.Val != i {
				t.Fatal("incorrect entity val")
			}
		} else {
			if err != datastore.ErrNoSuchEntity {
				t.Fatal("expected datastore.ErrNoSuchEntity")
			}
		}
	}
	b.Close()
}

func TestPutBatcher(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const loops = int64(50)
	type Entity struct {
		Val int64
	}

	b := newBatcher(c, dsPutExecutor)
	for i := int64(1); i < loops; i++ {
		if i%2 == 0 {
			key := datastore.NewKey(c, "Test", "", i, nil)
			entity := &Entity{i}
			item := []interface{}{key, entity}
			if err := b.Add(item); err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := int64(1); i < loops; i++ {
		key := datastore.NewKey(c, "Test", "", i, nil)
		entity := &Entity{}
		err := datastore.Get(c, key, entity)

		if i%2 == 0 {
			if err != nil {
				t.Fatal("expected nil error")
			}
			if entity.Val != i {
				t.Fatal("incorrect entity val")
			}
		} else {
			if err != datastore.ErrNoSuchEntity {
				t.Fatal("expected datastore.ErrNoSuchEntity")
			}
		}
	}

	// Test incomplete keys turn to complete keys.
	keys := make([]*datastore.Key, loops)
	for i := int64(0); i < loops; i++ {
		key := datastore.NewIncompleteKey(c, "Test", nil)
		keys[i] = key
		entity := &Entity{i}
		item := []interface{}{key, entity}
		if err := b.Add(item); err != nil {
			t.Fatal(err)
		}
	}

	for _, key := range keys {
		if key.Incomplete() {
		}
	}
	b.Close()
}

func TestDeleteBatcher(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const loops = int64(50)
	type Entity struct {
		Val int64
	}

	keys := []*datastore.Key{}
	for i := int64(1); i < loops; i++ {
		if i%2 == 0 {
			key := datastore.NewKey(c, "Test", "", i, nil)
			keys = append(keys, key)
			if _, err := datastore.Put(c, key, &Entity{i}); err != nil {
				t.Fatal(err)
			}
		}
	}

	b := newBatcher(c, dsDeleteExecutor)
	for _, key := range keys {
		b.Add(key)
	}
	b.Close()

	// Check keys don't exist.
	for _, key := range keys {
		err := datastore.Get(c, key, &Entity{})
		if err != datastore.ErrNoSuchEntity {
			t.Fatal("delete did not work")
		}
	}
}
