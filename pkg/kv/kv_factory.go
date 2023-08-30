package metastore

import (
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Factory interface {
	NewMetaKv(rootPath string) kv.MetaKv
}

var clientHandler = NewKVClientHandler()

type ETCDFactory struct {
	etcdClient *clientv3.Client
}

func NewETCDFactory(cfg *paramtable.ServiceParam) (*ETCDFactory, error) {
	client, err := clientHandler.GrabETCD(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create ETCDFactory")
	}
	return &ETCDFactory{etcdClient: client}, nil

}

func (fact *ETCDFactory) NewMetaKv(rootPath string) kv.MetaKv {
	kv := etcdkv.NewEtcdKV(fact.etcdClient, rootPath)
	return kv
}

type TiKVFactory struct {
	tikvClient *txnkv.Client
}

func NewTiKVFactory(cfg *paramtable.ServiceParam) (*TiKVFactory, error) {
	client, err := clientHandler.GrabTiKVClient(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create TiKVFactory")
	}
	return &TiKVFactory{tikvClient: client}, nil

}

func (fact *TiKVFactory) NewMetaKv(rootPath string) kv.MetaKv {
	kv := tikv.NewTiKV(fact.tikvClient, rootPath)
	return kv
}
