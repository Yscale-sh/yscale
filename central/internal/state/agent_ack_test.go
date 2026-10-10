package state

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

func TestAgentCommandAckWaiters(t *testing.T) {
	agent := &Agent{}
	ackCh, unregister := agent.RegisterCommandAck("cmd-1")
	defer unregister()

	want := protocol.CommandAck{CommandID: "cmd-1", Success: true}
	if !agent.DeliverCommandAck(want) {
		t.Fatal("registered acknowledgement was not delivered")
	}
	if got := <-ackCh; !reflect.DeepEqual(got, want) {
		t.Fatalf("ack = %+v, want %+v", got, want)
	}
	if agent.DeliverCommandAck(want) {
		t.Fatal("duplicate acknowledgement must not resolve a second waiter")
	}

	_, cancel := agent.RegisterCommandAck("cmd-cancelled")
	cancel()
	if agent.DeliverCommandAck(protocol.CommandAck{CommandID: "cmd-cancelled"}) {
		t.Fatal("cancelled waiter still accepted an acknowledgement")
	}
}

func TestAgentCommandAckWaitersConcurrent(t *testing.T) {
	agent := &Agent{}
	const count = 64
	channels := make([]<-chan protocol.CommandAck, count)
	cleanups := make([]func(), count)
	for i := 0; i < count; i++ {
		channels[i], cleanups[i] = agent.RegisterCommandAck(fmt.Sprintf("cmd-%d", i))
		defer cleanups[i]()
	}

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			agent.DeliverCommandAck(protocol.CommandAck{CommandID: fmt.Sprintf("cmd-%d", i), Success: true})
		}(i)
	}
	wg.Wait()
	for i, ch := range channels {
		if ack := <-ch; !ack.Success || ack.CommandID != fmt.Sprintf("cmd-%d", i) {
			t.Fatalf("waiter %d received %+v", i, ack)
		}
	}
}
