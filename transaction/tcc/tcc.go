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

// persistence state of initialization
func (t *Tcc) Begin() {
	t.base.DisTransactionStateStore()
}

func (t *Tcc) Try() {
	t.logger.Debug("transaction store true")
}

// commit the trasaction and release the state
func (t *Tcc) Commit() {

	t.logger.Info("this is Tcc, I received ")
}

// commit the trasaction and release the state
func (t *Tcc) RollBack() {
	t.logger.Info("this is Tcc, I received ")
}
