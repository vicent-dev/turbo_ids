package storage

type Row interface {
	IsValid() bool  // if true this row will be included in data set
	String() string // serialization of result in the final file
}

// example - create here your documents mapping
// https://transform.tools/json-to-go

type Room struct {
	ID         string `bson:"_id"`
	ListingURL string `bson:"listing_url"`
	Name       string `bson:"name"`
	Summary    string `bson:"summary"`
	Space      string `bson:"space"`
}

func (r Room) IsValid() bool {
	// business logic validation
	return true
}

func (r Room) String() string {
	return r.ID
}
