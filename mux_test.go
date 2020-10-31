package main

import (
	"context"
	"testing"
	"time"

	"github.com/coreos/etcd/storage/storagepb"
	"go.etcd.io/etcd/clientv3"
)

func Test_serialize(t *testing.T) {
	watcher := make(chan clientv3.WatchResponse)
	output := make(chan string)

	defer close(watcher)
	defer close(output)

	go serialize(watcher, output)

	watches := []clientv3.WatchResponse{
		{
			Events: []*storagepb.Event{
				{Kv: &storagepb.KeyValue{Value: []byte("a")}},
				{Kv: &storagepb.KeyValue{Value: []byte("b")}},
			},
		},
		{
			Events: []*storagepb.Event{
				{Kv: &storagepb.KeyValue{Value: []byte("c")}},
			},
		},
	}

	pusher := func() {
		for _, w := range watches {
			watcher <- w
		}
	}
	go pusher()

	for _, w := range watches {
		for _, e := range w.Events {
			// Sleep a bit to allow channels to populate
			time.Sleep(time.Millisecond * 1)
			select {
			case out := <-output:
				if string(e.Kv.Value) != out {
					t.Error("In correct value", out, "not", string(e.Kv.Value))
				}
			default:
				t.Error("Failed to receive output. Should be", string(e.Kv.Value))
			}
		}
	}
}

func Test_mux(t *testing.T) {
	inputs := []chan string{
		make(chan string),
		make(chan string),
	}
	output := make(chan string)

	go mux(context.TODO(), inputs, output, ",")

	values := [][]string{
		{"a", "b", "a,b"},
		{"a", "c", "a,c"},
		{"d", "b", "d,b"},
	}
	for _, value := range values {
		for i, in := range inputs {
			in <- value[i]
		}

		time.Sleep(time.Millisecond * 1)
		select {
		case out := <-output:
			if value[2] != out {
				t.Error("In correct value", out, "not", value[2])
			}
		default:
			t.Error("Failed to receive output. Should be", value[2])
		}
	}
}
