package main

import (
	"github.com/tikv/client-go/rawkv"
	"context"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/key"
	"fmt"
)

type TiKVClient struct {
	rawCli *rawkv.Client
	txnCli *txnkv.Client
}

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func NewClient() TiKVClient {
	rawCli, err := rawkv.NewClient(context.TODO(), []string{"pd1:2379"}, config.Default())
	if err != nil {
		panic(err)
	}

	txnCli, err := txnkv.NewClient(context.TODO(), []string{"pd1:2379"}, config.Default())
	if err != nil {
		panic(err)
	}
	return TiKVClient{rawCli, txnCli}
}

func (c *TiKVClient) Put(k, v []byte) error {
	return c.rawCli.Put(context.TODO(), k, v)
}

func (c *TiKVClient) Get(k []byte) ([]byte, error) {
	return c.rawCli.Get(context.TODO(), k)
}

func (c *TiKVClient) Delete(k []byte) error {
	return c.rawCli.Delete(context.TODO(), k)
}

func (c *TiKVClient) Scan(startKey []byte, endKey []byte, limit int) ([]KV, error) {
	ks, vs, err := c.rawCli.Scan(context.TODO(), startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	var ret []KV
	for i, k := range ks {
		ret = append(ret, KV{K: k, V: vs[i]})
	}
	return ret, nil
}

// key1 val1 key2 val2 ...
func (c *TiKVClient) TxPut(args ...[]byte) error {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return err
	}

	for i := 0; i < len(args); i += 2 {
		key, val := args[i], args[i+1]
		err := tx.Set(key, val)
		if err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}

func (c *TiKVClient) TxGet(k []byte) (KV, error) {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return KV{}, err
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil {
		return KV{}, err
	}
	return KV{K: k, V: v}, nil
}

func (c *TiKVClient) TxDelete(keys ...[]byte) error {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	for _, key := range keys {
		err := tx.Delete(key)
		if err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}

func (c *TiKVClient) TxScan(keyPrefix []byte, limit int) ([]KV, error) {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(keyPrefix), nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() && limit > 0 {
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		limit--
		it.Next(context.TODO())
	}
	return ret, nil
}

func main() {
	cli := NewClient()
	key := []byte("Khehe")
	value := []byte("Vhehe")
	cli.Put(key, value)
	v, err := cli.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))
}
