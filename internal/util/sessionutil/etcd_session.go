package sessionutil

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type EtcdSession struct {
	ctx               context.Context
	client            *clientv3.Client
	WatchSessionKeyCh clientv3.WatchChan
	KeepAliveChanel   <-chan *clientv3.LeaseKeepAliveResponse
}

// NewEtcdKV creates a new etcd kv.
func NewEtcdSession(client *clientv3.Client, ctx context.Context) (*EtcdSession, error) {
	log.Debug("Session try to connect to etcd")
	ctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	// Check if etcd is running and healthy, if error, etcd not in healthy state
	if _, err := client.Get(ctx2, "health"); err != nil {
		return nil, err
	}
	return &EtcdSession{
		ctx:    ctx,
		client: client,
	}, nil
}

func (s *EtcdSession) CompareVersionAndSwap(key string, version interface{}, replacement_val string) (success bool, err error) {
	res, err := s.client.Txn(s.ctx).If(clientv3.Compare(clientv3.Version(key), "=", version)).Then(clientv3.OpPut(key, replacement_val)).Commit()
	success = res.Succeeded
	return success, err
}

func (s *EtcdSession) CompareVersionAndSwapWithLease(key string, version interface{}, replacement_val string, leaseID int64) (success bool, err error) {
	res, err := s.client.Txn(s.ctx).If(clientv3.Compare(clientv3.Version(key), "=", version)).Then(clientv3.OpPut(key, replacement_val, clientv3.WithLease(clientv3.LeaseID(leaseID)))).Commit()
	success = res.Succeeded
	return success, err
}

func (s *EtcdSession) Get(key string) (value *string, revision *int64, err error) {
	getResp, err := s.client.Get(s.ctx, key)
	if err != nil {
		log.Warn("Session get etcd key error", zap.String("key", key), zap.Error(err))
		return nil, nil, err
	}
	revision = &getResp.Header.Revision
	// If there is no ID then wait for the CheckIDExist to finish
	if getResp.Count <= 0 {
		log.Warn("Session there is no value", zap.String("key", key))
		return nil, revision, nil
	}
	val_var := string(getResp.Kvs[0].Value)
	value = &val_var
	return value, revision, nil
}

func (s *EtcdSession) Watch(key string, revision int64) {
	s.WatchSessionKeyCh = s.client.Watch(s.ctx, key, clientv3.WithRev(revision))
}

// func (s *EtcdSession) WatchParse(chan )

func (s *EtcdSession) Grant(ttl int64) (int64, error) {
	res, err := s.client.Grant(s.ctx, ttl)
	val := int64(res.ID)
	return val, err
}

// TODO: Either wrap Keepalive witha goroutine to parse and return standard messages or
// just use s.KeepAliveChannel in the switch with session_util
func (s *EtcdSession) KeepAlive(keepAliveCtx context.Context, leaseID int64) (err error) {
	s.KeepAliveChanel, err = s.client.KeepAlive(keepAliveCtx, clientv3.LeaseID(leaseID))
	return err
}
