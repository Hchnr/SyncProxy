package etcdapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

var etcdClient *clientv3.Client

func init() {
	fmt.Println("init() in etcdapi here")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	etcdClient = cli
	if err != nil {
		// handle error!
		log.Fatalf("create etcd client error: %v", err)
	}
	// client closed after program exit
	// defer cli.Close()
}

func Put(key string, value string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	put_resp, err := etcdClient.Put(ctx, key, value)
	if err != nil {
		if err == context.Canceled {
			// ctx is canceled by another routine
			log.Fatalf("ctx is canceled by another routine: %v", err)
		} else if err == context.DeadlineExceeded {
			// ctx is attached with a deadline and it exceeded
			log.Fatalf("ctx is attached with a deadline is exceeded: %v", err)
		} else if err == rpctypes.ErrEmptyKey {
			// client-side error: key is not provided
			log.Fatalf("client-side error: %v", err)
		} else {
			// bad cluster endpoints, which are not etcd servers
			log.Fatalf("bad cluster endpoints, which are not etcd servers: %v", err)
		}
	}
	fmt.Println("put_resp:", put_resp)

	resp := make([]byte, 32)
	binary.BigEndian.PutUint64(resp, put_resp.Header.ClusterId)
	binary.BigEndian.PutUint64(resp[8:], put_resp.Header.MemberId)
	binary.BigEndian.PutUint64(resp[16:], uint64(put_resp.Header.Revision))
	binary.BigEndian.PutUint64(resp[24:], put_resp.Header.RaftTerm)
	return resp
}

func Get(key string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	get_resp, err := etcdClient.Get(ctx, key)
	if err != nil {
		// with etcd clientv3 <= v3.3
		if err == context.Canceled {
			// grpc balancer calls 'Get' with an inflight client.Close
		} 
		// with etcd clientv3 >= v3.4
		if clientv3.IsConnCanceled(err) {
			// gRPC client connection is closed
		}
	}
	// use the response
	fmt.Println("get_resp ==>", get_resp)
	return string(get_resp.Kvs[0].Value)
}

func GetSortedPrefix(key string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	get_resp, err := etcdClient.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		// with etcd clientv3 <= v3.3
		if err == context.Canceled {
			// grpc balancer calls 'Get' with an inflight client.Close
		} 
		// with etcd clientv3 >= v3.4
		if clientv3.IsConnCanceled(err) {
			// gRPC client connection is closed
		}
	}
	var resp []byte
	for _, ev := range get_resp.Kvs {
		resp = append(resp, []byte(ev.Key)...)
		resp = append(resp, 0)
		resp = append(resp, []byte(ev.Value)...)
		resp = append(resp, 0)
		fmt.Printf("get_resp prefix ==> %s : %s\n", ev.Key, ev.Value)
	}
	return resp
}

// WithRange sets the comparison to scan the range [key, end).
func GetWithRange(key string, end string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	get_resp, err := etcdClient.Get(ctx, key, clientv3.WithRange(end), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		// with etcd clientv3 <= v3.3
		if err == context.Canceled {
			// grpc balancer calls 'Get' with an inflight client.Close
		} 
		// with etcd clientv3 >= v3.4
		if clientv3.IsConnCanceled(err) {
			// gRPC client connection is closed
		}
	}
	var resp []byte
	for _, ev := range get_resp.Kvs {
		resp = append(resp, []byte(ev.Key)...)
		resp = append(resp, 0)
		resp = append(resp, []byte(ev.Value)...)
		resp = append(resp, 0)
		fmt.Printf("get_resp range ==> %s : %s\n", ev.Key, ev.Value)
	}
	return resp
}

/* the number of returned keys is bounded by limit. */
func GetWithLimit(key string, limit int64) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	get_resp, err := etcdClient.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithLimit(limit))
	if err != nil {
		// with etcd clientv3 <= v3.3
		if err == context.Canceled {
			// grpc balancer calls 'Get' with an inflight client.Close
		} 
		// with etcd clientv3 >= v3.4
		if clientv3.IsConnCanceled(err) {
			// gRPC client connection is closed
		}
	}
	var resp []byte
	for _, ev := range get_resp.Kvs {
		resp = append(resp, []byte(ev.Key)...)
		resp = append(resp, 0)
		resp = append(resp, []byte(ev.Value)...)
		resp = append(resp, 0)
		fmt.Printf("get_resp limit ==> %s : %s\n", ev.Key, ev.Value)
	}
	return resp
}

func Watch(key string) chan []byte {
	resps := make(chan []byte, 4096)
	go func() {
		ch := etcdClient.Watch(context.Background(), key)
		for wresp := range ch {
			for _, ev := range wresp.Events {
				fmt.Printf("Watch() %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				var resp []byte
				if ev.Type == 0 {
					resp = append(resp, 0)
				} else if ev.Type == 1 {
					resp = append(resp, 1)
				} else {
					resp = append(resp, 2)
				}
				resp = append(resp, []byte(ev.Kv.Key)...)
				resp = append(resp, 0)
				resp = append(resp, []byte(ev.Kv.Value)...)
				resp = append(resp, 0)
				resps <- resp
			}
		}
	} ()
	return resps
}


