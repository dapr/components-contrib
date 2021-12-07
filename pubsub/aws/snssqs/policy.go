package snssqs

type arnEquals struct {
	AwsSourceArn string `json:"aws:SourceArn"`
}

type condition struct {
	ArnEquals arnEquals
}

type statement struct {
	Effect    string
	Principal string
	Action    string
	Resource  string
	Condition condition
}

type policy struct {
	Version   string
	Statement []statement
}

func (p *policy) statementExists(other *statement) bool {
	for _, s := range p.Statement {
		if s.Effect == other.Effect &&
			s.Principal == other.Principal &&
			s.Action == other.Action &&
			s.Resource == other.Resource &&
			s.Condition.ArnEquals.AwsSourceArn == other.Condition.ArnEquals.AwsSourceArn {
			return true
		}
	}

	return false
}

func (p *policy) addStatement(other *statement) {
	p.Statement = append(p.Statement, *other)
}
