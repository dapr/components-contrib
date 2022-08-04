package tcc

import (
	"github.com/dapr/components-contrib/transaction"
	"github.com/dapr/kit/logger"
)

type Tcc struct {
	logger logger.Logger
}

func NewTccTransaction(logger logger.Logger) *Tcc {
	d := &Tcc{
		logger: logger,
	}

	return d
}

func (t *Tcc) Init(metadata transaction.Metadata) {

}

func (t *Tcc) Begin() {
	t.logger.Info("this is Tcc, I received ")
}

func (t *Tcc) Commit() {
	t.logger.Info("this is Tcc, I received ")
}

func (t *Tcc) RollBack() {
	t.logger.Info("this is Tcc, I received ")
}
