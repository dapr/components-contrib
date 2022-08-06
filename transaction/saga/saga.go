package saga

import (
	"github.com/dapr/components-contrib/transaction"
	"github.com/dapr/kit/logger"
)

type Saga struct {
	logger logger.Logger
}

func NewSagaTransaction(logger logger.Logger) *Saga {
	d := &Saga{
		logger: logger,
	}

	return d
}

func (s *Saga) Init(metadata transaction.Metadata) {

}

func (s *Saga) Begin() {
	s.logger.Info("this is Saga, I received ")
}

// save the evey sub transaction state
func (s *Saga) Try() {

	s.logger.Info("this is Saga, I received ")

}

// commit the trasaction and release the state
func (s *Saga) Commit(info string) {

	s.logger.Info("this is Saga, I received ")
}

func (s *Saga) RollBack(info string) {
	s.logger.Info("this is Saga, I received ")
}
