package transaction

type Transaction interface {

	// Init this component.
	Init(metadata Metadata)

	// Begin a distribute transaction
	Begin()

	// try to lock the transaction resource
	Try()

	// commit a distribute transaction
	Commit()

	// rooback a distribute transaction
	RollBack()
}
