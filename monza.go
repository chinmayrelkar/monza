
package monza

import (
	"context"
	"encoding/json"
	"time"
)


type Destination interface{
	Setup(ctx context.Context) error
	Record(ctx context.Context, event Event)
	Teardown(ctx context.Context)
}


type ServiceID string

type Event struct {
	Event      string      `json:"event,omitempty"`
	Data       interface{} `json:"data,omitempty"`
	ID         int64       `json:"id,omitempty"`
	ClientTime time.Time   `json:"client_time,omitempty"`
	IPAddr     string      `json:"ip_addr"`
	ServiceID ServiceID 		`json:"service_id"`
}

func (e Event) JSON() []byte {
	eventAsJSONStringBytes, err := json.Marshal(e)
	if err != nil {
		return nil
	}
	return eventAsJSONStringBytes
}



type Client interface {
	RegisterDestination(ctx context.Context, destination Destination) error
	Record(events Event)
	Teardown(ctx context.Context)
}

var instance Client

func Get(ctx context.Context, config Config) Client {
	if instance != nil {
		return instance
	}
	c := &client{
		Config: config,
		listener: make(chan Event),
		quit: make(chan interface{}),
	}
	c.startListening(ctx)
	instance = c
	return instance
}


type Config struct {
	IpAddress    string
	Destinations []Destination
}

type client struct {
	listener chan Event
	Config
	quit chan interface{}
}



func (c *client) RegisterDestination(ctx context.Context, destination Destination) error {
	err := destination.Setup(ctx)
	if err != nil {
		return err
	}
	c.Destinations = append(c.Destinations, destination)
	return nil
}

func (c *client) Record(event Event) {
	c.listener <- event
}

// Teardown implements Client.
func (c *client) Teardown(ctx context.Context) {
	c.quit <- nil
	for _, destination := range c.Destinations{
		destination.Teardown(ctx)
	}
}

func (c *client) startListening(ctx context.Context) {
	for {
		select {
		case <- c.quit:
			return
		default:
			events := <-c.listener
			for _, destination := range c.Destinations {
				destination.Record(ctx, events)
			}
		}
	}
}
