package transaction

type Transaction interface {
	// Init this component.
	Init(metadata Metadata)

	// begin a distribute transaction
	Begin()

	// commit a distribute transaction
	Commit()

	// rooback a distribute transaction
	RollBack()
}
