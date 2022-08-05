package tcc

import (
	"github.com/dapr/components-contrib/transaction"
	"github.com/dapr/kit/logger"
)

type Tcc struct {
	base   *transaction.TransactionStateStore
	logger logger.Logger
}

func NewTccTransaction(logger logger.Logger) *Tcc {
	base := &transaction.TransactionStateStore{}
	t := &Tcc{
		logger: logger,
		base:   base,
	}
	return t
}

func (t *Tcc) Init(metadata transaction.Metadata) {
	t.logger.Debug("init tranaction: tcc")
	t.base.InitTransactionStateStore(metadata)
}

func (t *Tcc) Try() {

	t.base.SubTransactionStateStore()
	t.logger.Debug("transaction store true")
}

func (t *Tcc) Commit() {
	t.logger.Info("this is Tcc, I received ")
}

func (t *Tcc) RollBack() {
	t.logger.Info("this is Tcc, I received ")
}
