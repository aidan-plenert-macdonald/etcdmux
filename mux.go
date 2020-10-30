package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/etcd-io/etcd/clientv3"
)

const usage = `etcd MUX
Usage:
./etcdmux

Then in separate terminal run,
$ etcdctl put mux/c b,a  # Creates mux
$ etcdctl put a 1
$ etcdctl put b 2

$ etcdctl get c  # Get output
1,2
`

func main() {
	fmt.Println(usage)
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"localhost:2379"},
	})
	if err != nil {
		log.Fatal("Error creating client", err)
		return
	}

	prefix, ok := os.LookupEnv("ETCDMUX_PREFIX")
	if !ok {
		prefix = "mux/"
	}

	separator, ok := os.LookupEnv("ETCDMUX_SEPARATOR")
	if !ok {
		separator = ","
	}

	watcher := client.Watcher.Watch(
		context.TODO(),
		prefix,
		clientv3.WithPrefix(),
	)
	contexts := make(map[string]context.CancelFunc, 5)
	for w := range watcher {
		for _, e := range w.Events {
			dest := strings.TrimPrefix(string(e.Kv.Key), prefix)
			srcs := strings.Split(string(e.Kv.Value), separator)
			log.Println("Recieved", e.Type, "destination", dest, "from", srcs)

			if cancel, ok := contexts[dest]; ok {
				cancel()
			}

			ctx, cancel := context.WithCancel(context.Background())
			contexts[dest] = cancel

			inputs := make([]chan string, len(srcs))
			output := make(chan string)
			defer close(output)

			for i, src := range srcs {
				inputs[i] = make(chan string)
				defer close(inputs[i])

				go serialize(
					client.Watcher.Watch(ctx, src, clientv3.WithPrefix()),
					inputs[i],
				)
			}
			go mux(ctx, inputs, output, separator)
			go put(output, dest, client)
		}
	}
}

func serialize(watcher clientv3.WatchChan, output chan string) {
	for w := range watcher {
		for _, e := range w.Events {
			output <- string(e.Kv.Value)
		}
	}
}

func mux(ctx context.Context, inputs []chan string, output chan string, separator string) {
	log.Println("Starting Mux")
	for {
		out := ""
		for _, input := range inputs {
			select {
			case v := <-input:
				out += v + separator
			case <-ctx.Done():
				log.Println("Exiting mux")
				return
			}
		}
		output <- out[:len(out)-len(separator)]
	}
}

func put(output chan string, dest string, client *clientv3.Client) {
	for out := range output {
		log.Println("Put", out, dest)
		client.Put(context.Background(), dest, out)
	}
}
