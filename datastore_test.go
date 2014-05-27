package nds_test

import (
    "testing"
    "appengine/aetest"
    "appengine/datastore"
    "github.com/qedus/nds"
)

type testEntity struct {
    Val int
}

type TestInterface interface {
    GetVal() int
}

func (te testEntity) GetVal() int {
    return te.Val
}

func TestPutGetStruct(t *testing.T) {

	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

    cc := nds.NewContext(c)

    keys := []*datastore.Key{datastore.NewKey(cc, "Test", "", 3, nil)}
    src := []testEntity{testEntity{3}}

    if newKeys, err := nds.PutMulti(cc, keys, src); err != nil {
        t.Fatal(err)
    } else if !newKeys[0].Equal(keys[0]) {
        t.Fatal("keys not equal")
    }

    dst := make([]testEntity, 1)
    if err := nds.GetMulti(cc, keys, dst); err != nil {
        t.Fatal(err)
    } else if dst[0].Val != src[0].Val {
        t.Fatal("entities not the same")
    }

    // Get again from cache.
    if err := nds.GetMulti(cc, keys, dst); err != nil {
        t.Fatal(err)
    } else if dst[0].Val != src[0].Val {
        t.Fatal("entities not the same")
    }
}

func TestPutGetPointer(t *testing.T) {

	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

    cc := nds.NewContext(c)

    keys := []*datastore.Key{datastore.NewKey(cc, "Test", "", 3, nil)}
    src := []*testEntity{&testEntity{3}}

    if newKeys, err := nds.PutMulti(cc, keys, src); err != nil {
        t.Fatal(err)
    } else if !newKeys[0].Equal(keys[0]) {
        t.Fatal("keys not equal")
    }

    dst := []*testEntity{&testEntity{}}
    if err := nds.GetMulti(cc, keys, dst); err != nil {
        t.Fatal(err)
    } else if dst[0].Val != src[0].Val {
        t.Fatal("entities not the same")
    }

    // Get again from cache.
    if err := nds.GetMulti(cc, keys, dst); err != nil {
        t.Fatal(err)
    } else if dst[0].Val != src[0].Val {
        t.Fatal("entities not the same")
    }
}

func TestPutGetInterface(t *testing.T) {

	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

    cc := nds.NewContext(c)

    keys := []*datastore.Key{datastore.NewKey(cc, "Test", "", 3, nil)}
    src := []TestInterface{&testEntity{3}}

    if newKeys, err := nds.PutMulti(cc, keys, src); err != nil {
        t.Fatal(err)
    } else if !newKeys[0].Equal(keys[0]) {
        t.Fatal("keys not equal")
    }

    dst := []TestInterface{&testEntity{}}
    if err := nds.GetMulti(cc, keys, dst); err != nil {
        t.Fatal(err)
    } else if dst[0].(*testEntity).Val != src[0].(*testEntity).Val {
        t.Fatal("entities not the same")
    }

    // Get again from local cache.
    if err := nds.GetMulti(cc, keys, dst); err != nil {
        t.Fatal(err)
    } else if dst[0].(*testEntity).Val != src[0].(*testEntity).Val {
        t.Fatal("entities not the same")
    }
}

func TestPutGetPropertyLoadSaver(t *testing.T) {

	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

    cc := nds.NewContext(c)

    keys := []*datastore.Key{datastore.NewKey(cc, "Test", "", 3, nil)}
    src := []datastore.PropertyList{
        datastore.PropertyList{datastore.Property{Name:"Val", Value:int64(3)}}}

    if newKeys, err := nds.PutMulti(cc, keys, src); err != nil {
        t.Fatal(err)
    } else if !newKeys[0].Equal(keys[0]) {
        t.Fatal("keys not equal")
    }

    dst := []datastore.PropertyList{datastore.PropertyList{}}
    if err := nds.GetMulti(cc, keys, dst); err != nil {
        t.Fatal(err)
    } else if dst[0][0].Name != src[0][0].Name {
        t.Fatal("entities not the same")
    }

    // Get again from cache.
    if err := nds.GetMulti(cc, keys, dst); err != nil {
        t.Fatal(err)
    } else if dst[0][0].Name != src[0][0].Name {
        t.Fatal("entities not the same")
    }
}


