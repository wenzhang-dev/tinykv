package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
    "github.com/Connor1996/badger"
    "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
    DB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
    return &StandAloneStorage{
        DB: engine_util.CreateDB(conf.DBPath, false),
    }
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
    txn := s.DB.NewTransaction(false)
    return newStandAloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
    write_batch := new(engine_util.WriteBatch)

    for _, op := range batch {
        if _, ok := op.Data.(storage.Put); ok {
            write_batch.SetCF(op.Cf(), op.Key(), op.Value())
        } else {
            write_batch.DeleteCF(op.Cf(), op.Key())
        }
    }

    return write_batch.WriteToDB(s.DB)
}

type StandAloneStorageReader struct {
    Txn *badger.Txn
}

func newStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
    return &StandAloneStorageReader{
        Txn: txn,
    }
}

func (r *StandAloneStorageReader) Close() {
    r.Txn.Discard()
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
    val, err := engine_util.GetCFFromTxn(r.Txn, cf, key)
    if err == badger.ErrKeyNotFound {
        return nil, nil
    }
    return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
    return engine_util.NewCFIterator(cf, r.Txn)
}
