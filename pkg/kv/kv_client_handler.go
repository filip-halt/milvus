package metastore

import (
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	metaTypeEtcd = util.MetaStoreTypeEtcd
	metaTypeTikv = util.MetaStoreTypeTiKV
)

type KVClientHandler struct {
	mu         sync.Mutex
	tikvClient *txnkv.Client
	etcdClient *clientv3.Client
}

func NewKVClientHandler() *KVClientHandler {
	return &KVClientHandler{
		mu:         sync.Mutex{},
		tikvClient: nil,
		etcdClient: nil,
	}
}

func (handler *KVClientHandler) GrabETCD(cfg *paramtable.ServiceParam) (*clientv3.Client, error) {
	handler.mu.Lock()
	defer handler.mu.Unlock()

	if handler.etcdClient == nil {
		new_client, err := createETCDClient(cfg)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to create ETCD client")
		}
		handler.etcdClient = new_client
	}
	return handler.etcdClient, nil
}

func createETCDClient(cfg *paramtable.ServiceParam) (*clientv3.Client, error) {
	client, err := etcd.GetEtcdClient(
		cfg.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		cfg.EtcdCfg.EtcdUseSSL.GetAsBool(),
		cfg.EtcdCfg.Endpoints.GetAsStrings(),
		cfg.EtcdCfg.EtcdTLSCert.GetValue(),
		cfg.EtcdCfg.EtcdTLSKey.GetValue(),
		cfg.EtcdCfg.EtcdTLSCACert.GetValue(),
		cfg.EtcdCfg.EtcdTLSMinVersion.GetValue())
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (handler *KVClientHandler) GrabTiKVClient(cfg *paramtable.ServiceParam) (*txnkv.Client, error) {
	handler.mu.Lock()
	defer handler.mu.Unlock()

	if handler.tikvClient == nil {
		new_client, err := createTiKVClient(cfg)
		if err != nil {
			return nil, errors.Wrapf(err, "create tikv client failed")
		}
		handler.tikvClient = new_client
	}
	return handler.tikvClient, nil
}

func createTiKVClient(cfg *paramtable.ServiceParam) (*txnkv.Client, error) {
	client, err := txnkv.NewClient([]string{cfg.TiKVCfg.Endpoints.GetValue()})
	return client, err
}
