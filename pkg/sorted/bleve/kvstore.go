package bleve

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/blevesearch/bleve/index/store"

	"perkeep.org/pkg/sorted"
)

type kvStore struct {
	txr sorted.TransactionalReader
}

type kvReader struct {
	tx sorted.ReadTransaction
}

type kvRangeIter struct {
	it sorted.Iterator
	ok bool
}

type kvPrefixIter struct {
	prefix string
	ok     bool
	it     sorted.Iterator
}

type kvWriter struct {
	store *kvStore
}

type kvBatch struct {
	batch sorted.BatchMutation
}

func (kv *kvStore) Writer() (store.KVWriter, error) {
	return &kvWriter{store: kv}, nil
}

func (kv *kvStore) Reader() (store.KVReader, error) {
	return &kvReader{tx: kv.txr.BeginReadTx()}, nil
}

func (r *kvReader) Get(key []byte) ([]byte, error) {
	val, err := r.tx.Get(toKey(key))
	if err != nil {
		return []byte{}, err
	}
	return fromVal(val)
}

func (r *kvReader) MultiGet(keys [][]byte) ([][]byte, error) {
	ret := make([][]byte, len(keys))
	for i, k := range keys {
		v, err := r.Get(k)
		if err != nil {
			return nil, err
		}
		ret[i] = v
	}
	return ret, nil
}

func (r *kvReader) PrefixIterator(prefix []byte) store.KVIterator {
	key := toKey(prefix)
	return &kvPrefixIter{
		prefix: key,
		it:     r.tx.Find(toKey(prefix), endKey),
	}
}

func (it *kvPrefixIter) Next() {
	if it.ok {
		it.ok = it.it.Next()
		it.firstPrefixed()
	}
}

func (it *kvPrefixIter) Valid() bool {
	return it.ok
}

func (it *kvPrefixIter) firstPrefixed() {
	for it.Valid() && !strings.HasPrefix(it.it.Key(), it.prefix) {
		it.ok = it.it.Next()
	}
}

func (it *kvPrefixIter) Seek(key []byte) {
	seekCommon(it, key)
}

func (it *kvPrefixIter) Key() []byte {
	return keyCommon(it.it, it.ok)
}

func (it *kvPrefixIter) Value() []byte {
	return valueCommon(it.it, it.ok)
}

func (it *kvPrefixIter) Current() ([]byte, []byte, bool) {
	return currentCommon(it)
}

func (it *kvPrefixIter) Close() error {
	return it.it.Close()
}

func (r *kvReader) RangeIterator(start, end []byte) store.KVIterator {
	return &kvRangeIter{
		it: r.tx.Find(toKey(start), toKey(end)),
		ok: true,
	}
}

func (it *kvRangeIter) Seek(key []byte) {
	seekCommon(it, key)
}

func (it *kvRangeIter) Next() {
	if !it.ok {
		return
	}
	it.ok = it.it.Next()
}

func (it *kvRangeIter) Key() []byte {
	return keyCommon(it.it, it.ok)
}

func (it *kvRangeIter) Value() []byte {
	return valueCommon(it.it, it.ok)
}

func (it *kvRangeIter) Valid() bool {
	return it.ok
}

func (it *kvRangeIter) Current() ([]byte, []byte, bool) {
	return currentCommon(it)
}

func (it *kvRangeIter) Close() error {
	return it.it.Close()
}

func (kv *kvStore) Close() error {
	return kv.txr.Close()
}

func (w *kvWriter) NewBatch() store.KVBatch {
	return &kvBatch{batch: w.store.txr.BeginBatch()}
}

func (w *kvWriter) NewBatchEx(opts store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return []byte{}, w.NewBatch(), nil
}

func (w *kvWriter) ExecuteBatch(batch store.KVBatch) error {
	return w.store.txr.CommitBatch(batch.(*kvBatch).batch)
}

func (*kvWriter) Close() error {
	return nil
}
func (r *kvReader) Close() error {
	return r.tx.Close()
}

func (b *kvBatch) Set(key, val []byte) {
	b.batch.Set(toKey(key), toValue(val))
}

func (b *kvBatch) Delete(key []byte) {
	b.batch.Delete(toKey(key))
}

func (b *kvBatch) Close() error {
	return b.batch.Close()
}

// A value that is after all possible bleve keys:
const endKey = "bleve:z"

// Convert a bleve key into a sorted.KeyValue key.
func toKey(b []byte) string {
	return fmt.Sprintf("bleve:%x", b)
}

// Convert a sorted.KeyValue key into a bleve key.
func fromKey(key string) ([]byte, error) {
	ret := []byte{}
	_, err := fmt.Sscanf(key, "bleve:%x", &ret)
	return ret, err
}

// Convert a bleve value into a sorted.KeyValue value.
func toValue(b []byte) string {
	return fmt.Sprintf("%x", b)
}

// Convert a sorted.KeyValue value into a bleve value.
func fromVal(val string) ([]byte, error) {
	ret := []byte{}
	_, err := fmt.Sscanf(val, "%x", &ret)
	return ret, err
}

// Common hepers for kvPrefixIter and kvRangeIter:

func currentCommon(it store.KVIterator) ([]byte, []byte, bool) {
	return it.Key(), it.Value(), it.Valid()
}

func keyCommon(it sorted.Iterator, ok bool) []byte {
	if !ok {
		return []byte{}
	}
	key, err := fromKey(it.Key())
	if err != nil {
		return []byte{}
	}
	return key
}

func valueCommon(it sorted.Iterator, ok bool) []byte {
	if !ok {
		return []byte{}
	}
	val, err := fromVal(it.Value())
	if err != nil {
		return []byte{}
	}
	return val
}

func seekCommon(it store.KVIterator, target []byte) {
	it.Next()
	for it.Valid() {
		cur := it.Key()
		if bytes.Compare(cur, target) <= 0 {
			return
		}
	}
}
