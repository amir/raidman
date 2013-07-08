// Go Riemann client
package raidman

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"

	pb "code.google.com/p/goprotobuf/proto"
	"github.com/swdunlop/raidman/proto"
)

type network interface {
	Send(message *proto.Msg, conn net.Conn) (*proto.Msg, error)
}

type tcp struct{}

type udp struct{}

// Client represents a connection to a Riemann server
type Client struct {
	sync.Mutex
	net        network
	connection net.Conn
}

// An Event represents a single Riemann event
type Event struct {
	Ttl         float32
	Time        int64
	Tags        []string
	Host        string // Defaults to os.Hostname()
	State       string
	Service     string
	Metric      interface{} // Could be Int, Float32, Float64
	Description string
}

// Dial establishes a connection to a Riemann server at addr, on the network
// netwrk.
//
// Known networks are "tcp", "tcp4", "tcp6", "udp", "udp4", and "udp6".
func Dial(netwrk, addr string) (c *Client, err error) {
	c = new(Client)

	var cnet network
	switch netwrk {
	case "tcp", "tcp4", "tcp6":
		cnet = new(tcp)
	case "udp", "udp4", "udp6":
		cnet = new(udp)
	default:
		return nil, fmt.Errorf("dial %q: unsupported network %q", netwrk, netwrk)
	}

	c.net = cnet
	c.connection, err = net.Dial(netwrk, addr)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (network *tcp) Send(message *proto.Msg, conn net.Conn) (*proto.Msg, error) {
	msg := &proto.Msg{}
	data, err := pb.Marshal(message)
	if err != nil {
		return msg, err
	}
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, uint32(len(data))); err != nil {
		return msg, err
	}
	if _, err = conn.Write(b.Bytes()); err != nil {
		return msg, err
	}
	if _, err = conn.Write(data); err != nil {
		return msg, err
	}
	var header uint32
	if err = binary.Read(conn, binary.BigEndian, &header); err != nil {
		return msg, err
	}
	response := make([]byte, header)
	if err = readFully(conn, response); err != nil {
		return msg, err
	}
	if err = pb.Unmarshal(response, msg); err != nil {
		return msg, err
	}
	if msg.GetOk() != true {
		return msg, errors.New(msg.GetError())
	}
	return msg, nil
}

func readFully(r io.Reader, p []byte) error {
	for len(p) > 0 {
		n, err := r.Read(p)
		p = p[n:]
		if err != nil {
			return err
		}
	}
	return nil
}

func (network *udp) Send(message *proto.Msg, conn net.Conn) (*proto.Msg, error) {
	data, err := pb.Marshal(message)
	if err != nil {
		return nil, err
	}
	if _, err = conn.Write(data); err != nil {
		return nil, err
	}

	return nil, nil
}

func eventToPbEvent(event *Event) (*proto.Event, error) {
	var e proto.Event

	if event.Host == "" {
		event.Host, _ = os.Hostname()
	}
	t := reflect.ValueOf(&e).Elem()
	s := reflect.ValueOf(event).Elem()
	typeOfEvent := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		value := reflect.ValueOf(f.Interface())
		if reflect.Zero(f.Type()) != value && f.Interface() != nil {
			name := typeOfEvent.Field(i).Name
			switch name {
			case "State", "Service", "Host", "Description":
				tmp := reflect.ValueOf(pb.String(value.String()))
				t.FieldByName(name).Set(tmp)
			case "Ttl":
				tmp := reflect.ValueOf(pb.Float32(float32(value.Float())))
				t.FieldByName(name).Set(tmp)
			case "Time":
				tmp := reflect.ValueOf(pb.Int64(value.Int()))
				t.FieldByName(name).Set(tmp)
			case "Tags":
				tmp := reflect.ValueOf(value.Interface().([]string))
				t.FieldByName(name).Set(tmp)
			case "Metric":
				switch reflect.TypeOf(f.Interface()).Kind() {
				case reflect.Int:
					tmp := reflect.ValueOf(pb.Int64(int64(value.Int())))
					t.FieldByName("MetricSint64").Set(tmp)
				case reflect.Float32:
					tmp := reflect.ValueOf(pb.Float32(float32(value.Float())))
					t.FieldByName("MetricF").Set(tmp)
				case reflect.Float64:
					tmp := reflect.ValueOf(pb.Float64(value.Float()))
					t.FieldByName("MetricD").Set(tmp)
				default:
					return nil, fmt.Errorf("Metric of invalid type (type %v)",
						reflect.TypeOf(f.Interface()).Kind())
				}
			}
		}
	}

	return &e, nil
}

func pbEventsToEvents(pbEvents []*proto.Event) []Event {
	var events []Event

	for _, event := range pbEvents {
		e := Event{
			State:       event.GetState(),
			Service:     event.GetService(),
			Host:        event.GetHost(),
			Description: event.GetDescription(),
			Ttl:         event.GetTtl(),
			Time:        event.GetTime(),
			Tags:        event.GetTags(),
		}
		if event.MetricF != nil {
			e.Metric = event.GetMetricF()
		} else if event.MetricD != nil {
			e.Metric = event.GetMetricD()
		} else {
			e.Metric = event.GetMetricSint64()
		}

		events = append(events, e)
	}

	return events
}

// Send sends an event to Riemann
func (c *Client) Send(event *Event) error {
	e, err := eventToPbEvent(event)
	if err != nil {
		return err
	}
	message := &proto.Msg{}
	message.Events = append(message.Events, e)
	c.Lock()
	defer c.Unlock()
	_, err = c.net.Send(message, c.connection)
	if err != nil {
		return err
	}

	return nil
}

// Query returns a list of events matched by query
func (c *Client) Query(q string) ([]Event, error) {
	switch c.net.(type) {
	case *udp:
		return nil, errors.New("Querying over UDP is not supported")
	}
	query := &proto.Query{}
	query.String_ = pb.String(q)
	message := &proto.Msg{}
	message.Query = query
	c.Lock()
	defer c.Unlock()
	response, err := c.net.Send(message, c.connection)
	if err != nil {
		return nil, err
	}
	return pbEventsToEvents(response.GetEvents()), nil
}

// Close closes the connection to Riemann
func (c *Client) Close() {
	c.Lock()
	c.connection.Close()
	c.Unlock()
}
