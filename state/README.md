# State Stores

State Stores provide a common way to interact with different data store implementations, and allow users to opt-in to advanced capabilities using defined metadata.

## Implementing a new State Store

A compliant state store needs to implement one or more interfaces: `Store` and `TransactionalStore`, defined in the [`store.go`](store.go) file.

See the [documentation site](https://docs.dapr.io/developing-applications/building-blocks/state-management/) for examples.  

## Implementing State Query API

State Store has an optional API for querying the state. 

Please refer to the [documentation site](https://docs.dapr.io/developing-applications/building-blocks/state-management/howto-state-query-api/) for API description and definition. 

```go
// Querier is an interface to execute queries.
type Querier interface {
        Query(req *QueryRequest) (*QueryResponse, error)
}
```

Below are the definitions of structures (including nested) for `QueryRequest` and `QueryResponse`.

```go
// QueryResponse is the request object for querying the state.
type QueryRequest struct {
        Query    query.Query       `json:"query"`
        Metadata map[string]string `json:"metadata,omitempty"`
}

type Query struct {
        Filters map[string]interface{} `json:"filter"`
        Sort    []Sorting              `json:"sort"`
        Page    Pagination             `json:"page"`

        // derived from Filters
        Filter Filter
}

type Sorting struct {
        Key   string `json:"key"`
        Order string `json:"order,omitempty"`
}

type Pagination struct {
        Limit int    `json:"limit"`
        Token string `json:"token,omitempty"`
}

// QueryResponse is the response object on querying state.
type QueryResponse struct {
        Results  []QueryItem       `json:"results"`
        Token    string            `json:"token,omitempty"`
        Metadata map[string]string `json:"metadata,omitempty"`
}

// QueryItem is an object representing a single entry in query results.
type QueryItem struct {
        Key   string  `json:"key"`
        Data  []byte  `json:"data"`
        ETag  *string `json:"etag,omitempty"`
        Error string  `json:"error,omitempty"`
}
```

Upon receiving the query request, Dapr validates it and transforms into object `Query`,
which, in turn, is passed on to the state store component.

The `Query` object has a member `Filter` that implements parsing interface per component as described below.

```go
type Filter interface {
	Parse(interface{}) error
}

type FilterEQ struct {
	Key string
	Val interface{}
}

type FilterIN struct {
	Key  string
	Vals []interface{}
}

type FilterAND struct {
	Filters []Filter
}

type FilterOR struct {
	Filters []Filter
}
```

To simplify the process of query translation, we leveraged [visitor design pattern](https://datacadamia.com/data/type/tree/visitor). A state store component developer would need to implement the `visit` method, and the runtime will use it to construct the native query statement.

```go
type Visitor interface {
	// returns "equal" expression
	VisitEQ(*FilterEQ) (string, error)
	// returns "in" expression
	VisitIN(*FilterIN) (string, error)
	// returns "and" expression
	VisitAND(*FilterAND) (string, error)
	// returns "or" expression
	VisitOR(*FilterOR) (string, error)
	// receives concatenated filters and finalizes the native query
	Finalize(string, *MidQuery) error
}
```

The Dapr runtime implements `QueryBuilder` object that takes in `Visitor` interface and constructs the native query.

```go
type QueryBuilder struct {
	visitor Visitor
}

func (h *QueryBuilder) BuildQuery(mq *MidQuery) error {...}
```

The last part is to implement `Querier` interface in the component:

```go
type Querier interface {
	Query(req *QueryRequest) (*QueryResponse, error)
}
```

A sample implementation might look like that:

```go
func (m *MyComponent) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	query := &Query{} // Query implements Visitor interface
	qbuilder := state.NewQueryBuilder(query)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}
	data, token, err := query.execute(ctx)
	if err != nil {
		return &state.QueryResponse{}, err
	}
	return &state.QueryResponse{
		Results:  data,
		Token:    token,
	}, nil
}
```

Some of the examples of State Query API implementation are [MongoDB](./mongodb/mongodb_query.go) and [CosmosDB](./azure/cosmosdb/cosmosdb_query.go) state store components.
