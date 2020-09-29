package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	kv *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	s.kv = db
	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	var a []*engine_util.BadgerIterator
	var b []*badger.Txn
	return &BadgerReader{ctx,s.kv,a,b}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(m.Data.(storage.Put).Cf,m.Data.(storage.Put).Key,m.Data.(storage.Put).Value)
		case storage.Delete:
			writeBatch.DeleteCF(m.Data.(storage.Put).Cf,m.Data.(storage.Put).Key)
		}
	}

	return writeBatch.WriteToDB(s.kv)

}

type BadgerReader struct {
	// Your Data Here (1).
	ctx *kvrpcpb.Context
	kv *badger.DB
	iters []*engine_util.BadgerIterator
	iterTxns []*badger.Txn

}

func (br *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	v, _ := engine_util.GetCF(br.kv,cf,key)
	return v,nil
}

func (br *BadgerReader) IterCF(cf string) engine_util.DBIterator{
	txn := br.kv.NewTransaction(false)
	//defer txn.Discard()
	iter := engine_util.NewCFIterator(cf,txn)
	iter.Rewind()
	br.iterTxns = append(br.iterTxns,txn)
	br.iters = append(br.iters,iter)
	return iter
}
func (br *BadgerReader) Close(){
	for  _,iter := range br.iters{
		iter.Close()
	}

	for _,txn := range br.iterTxns{
		txn.Discard()
	}
}
