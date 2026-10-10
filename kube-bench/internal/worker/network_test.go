package worker

import (
	"net"
	"testing"
	"time"
)

func TestWaitForTCPRetriesUntilListenerIsReady(t *testing.T) {
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}

	listenerReady := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		time.Sleep(150 * time.Millisecond)
		listener, err := net.Listen("tcp", address)
		if err != nil {
			serverDone <- err
			return
		}
		close(listenerReady)
		defer listener.Close()
		connection, err := listener.Accept()
		if err == nil {
			err = connection.Close()
		}
		serverDone <- err
	}()

	if err := waitForTCP(address, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-listenerReady:
	case <-time.After(time.Second):
		t.Fatal("listener did not start")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
