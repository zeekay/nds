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
	context appengine.Context

	executor executor

	payloadChan chan payload
	closedChan  chan bool
}

type executor func(appengine.Context, []interface{}) error

type payload struct {
	item    interface{}
	errChan chan error
}

var dsGetExecutor = func(c appengine.Context, items []interface{}) error {
	keys := make([]*datastore.Key, len(items))
	vals := make([]interface{}, len(items))
	for i, item := range items {
		keys[i] = item.([]interface{})[0].(*datastore.Key)
		vals[i] = item.([]interface{})[1]
	}
	return datastore.GetMulti(c, keys, vals)
}

var dsPutExecutor = func(c appengine.Context, items []interface{}) error {
	keys := make([]*datastore.Key, len(items))
	vals := make([]interface{}, len(items))
	for i, item := range items {
		keys[i] = item.([]interface{})[0].(*datastore.Key)
		vals[i] = item.([]interface{})[1]
	}
	completeKeys, err := datastore.PutMulti(c, keys, vals)
	for i, key := range keys {
		if key.Incomplete() {
			*key = *completeKeys[i]
		}
	}
	return err
}

var dsDeleteExecutor = func(c appengine.Context, items []interface{}) error {
	keys := make([]*datastore.Key, len(items))
	for i, item := range items {
		keys[i] = item.(*datastore.Key)
	}
	return datastore.DeleteMulti(c, keys)
}

func newBatcher(c appengine.Context, executor executor) *batcher {
	b := &batcher{
		context:  c,
		executor: executor,

		payloadChan: make(chan payload),
		closedChan:  make(chan bool),
	}
	go b.loop()
	return b
}

func (b *batcher) Close() {
	fmt.Println("Close called.")
	close(b.payloadChan)
	<-b.closedChan
}

func (b *batcher) Add(item interface{}) error {
	errChan := make(chan error)
	b.payloadChan <- payload{item, errChan}
	return <-errChan
}

func (b *batcher) loop() {
	items := []interface{}{}
	errChans := []chan error{}
	for {
		if len(items) == 0 {
			select {
			case payload, ok := <-b.payloadChan:
				if !ok {
					b.closedChan <- true
					return
				}
				items = append(items, payload.item)
				errChans = append(errChans, payload.errChan)
			}
		} else {
			select {
			case payload, ok := <-b.payloadChan:
				if !ok {
					b.closedChan <- true
					return
				}
				items = append(items, payload.item)
				errChans = append(errChans, payload.errChan)
			default:
				err := b.executor(b.context, items)
				if me, ok := err.(appengine.MultiError); ok {
					for i, errChan := range errChans {
						errChan <- me[i]
					}
				} else {
					for _, errChan := range errChans {
						errChan <- err
					}
				}
				items = []interface{}{}
				errChans = []chan error{}
			}
		}
	}
}
