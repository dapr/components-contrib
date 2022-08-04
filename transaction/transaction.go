package transaction

type Transaction interface {
	// Init this component.
	Init(metadata Metadata)

	Try()
	// commit a distribute transaction
	Commit()

	// rooback a distribute transaction
	RollBack()
}
