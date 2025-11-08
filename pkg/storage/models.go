package storage

type Row interface {
	String() string
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

func (r Room) String() string {
	return r.ID
}
