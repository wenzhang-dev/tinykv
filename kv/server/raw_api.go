package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
    reader, err := server.storage.Reader(req.Context)
    if err != nil {
        return nil, err
    }

    defer reader.Close()

    val, err := reader.GetCF(req.Cf, req.Key)
    if err != nil {
        return nil, err
    }

    // when key not found, val should be nil, which should not be considered an error
    if val == nil {
        return &kvrpcpb.RawGetResponse{
            NotFound: true,
        }, nil
    }

    return &kvrpcpb.RawGetResponse{
        Value: val,
    }, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
    var batch []storage.Modify

    putOp := storage.Modify{
        Data: storage.Put{
            Key: req.Key,
            Value: req.Value,
            Cf: req.Cf,
        },
    }

    batch = append(batch, putOp)
    err := server.storage.Write(req.Context, batch)
    if err != nil {
        return nil, err
    }

    return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
    var batch []storage.Modify

    delOp := storage.Modify{
        Data: storage.Delete{
            Key: req.Key,
            Cf: req.Cf,
        },
    }

    batch = append(batch, delOp)
    err := server.storage.Write(req.Context, batch)
    if err != nil {
        return nil, err
    }

    return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
    reader, err := server.storage.Reader(req.Context)
    if err != nil {
        return nil, err
    }

    defer reader.Close()

    iter := reader.IterCF(req.Cf)
    defer iter.Close()

    var kvs []*kvrpcpb.KvPair
    for iter.Seek(req.StartKey); iter.Valid() && len(kvs) < int(req.Limit); iter.Next() {
        item := iter.Item()

        kv := kvrpcpb.KvPair{}
        kv.Key = item.KeyCopy(nil)
        kv.Value, err = item.ValueCopy(nil)
        if err != nil {
            return nil, err
        }

        kvs = append(kvs, &kv)
    }

    return &kvrpcpb.RawScanResponse{
        Kvs: kvs,
    }, nil
}
