package nds

import (
	"appengine"
	"appengine/datastore"
	"fmt"
)

// batcher is used to batch related operations on datastore entities. It is
// designed to make the caching code easier to reason with as individual
// entities can be considered instead instead of slices of entities. Although
// for efficiency the datastore works best with batch gets, puts and deletes.
type batcher struct {
	c appengine.Context

	getTupChan    chan getTup
	getClosedChan chan bool
}

type getTup struct {
	key     *datastore.Key
	pl      datastore.PropertyList
	errChan chan error
}

func newBatcher(c appengine.Context) *batcher {
	b := &batcher{
		c: c,

		getTupChan:    make(chan getTup),
		getClosedChan: make(chan bool),
	}
	go b.datastoreGetLoop()
	return b
}

func (b *batcher) Close() {
	fmt.Println("Close called.")
	close(b.getTupChan)
	<-b.getClosedChan
}

func (b *batcher) DatastoreGet(key *datastore.Key,
	pl datastore.PropertyList) error {
	errChan := make(chan error)
	b.getTupChan <- getTup{key, pl, errChan}
	return <-errChan
}

func (b *batcher) datastoreGetLoop() {
	keys := []*datastore.Key{}
	pls := []datastore.PropertyList{}
	errChans := []chan error{}
	for {
		if len(keys) == 0 {
			select {
			case getTup, ok := <-b.getTupChan:
				if !ok {
					b.getClosedChan <- true
					return
				}
				keys = append(keys, getTup.key)
				pls = append(pls, getTup.pl)
				errChans = append(errChans, getTup.errChan)
			}
		} else {
			fmt.Println("keys", len(keys))
			select {
			case getTup, ok := <-b.getTupChan:
				if !ok {
					b.getClosedChan <- true
					return
				}
				keys = append(keys, getTup.key)
				pls = append(pls, getTup.pl)
				errChans = append(errChans, getTup.errChan)
			default:
				err := datastore.GetMulti(b.c, keys, pls)
				if me, ok := err.(appengine.MultiError); ok {
					for i, errChan := range errChans {
						errChan <- me[i]
					}
				} else {
					for _, errChan := range errChans {
						errChan <- err
					}
				}
				keys = []*datastore.Key{}
				pls = []datastore.PropertyList{}
				errChans = []chan error{}
			}
		}
	}
}
