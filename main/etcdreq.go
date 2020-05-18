package main
 
import (
    "encoding/binary"
    "fmt"
	"net"
	"github.com/hchnr/etcd-proxy/codec"
	"github.com/hchnr/etcd-proxy/config"
    "github.com/hchnr/etcd-proxy/etcdapi"
)

// etcdapi.Put(); 2 oprands
func etcdPut(msg []byte, conn net.Conn) {
	for i, b := range msg[1:] {
		if b == 0x00 {
			key := string(msg[1: i + 1])
			value := string(msg[i + 2: len(msg) - 1])
			if config.LocalConfig.IsDebug {
				fmt.Println("etcdapi.Put():", key, value)
			}
			/*  Response:
				uint64 cluster_id = 1;
				uint64 member_id = 2;
				int64 revision = 3;
				uint64 raft_term = 4;
			*/ 
			putResponse := etcdapi.Put(key, value)
			resp, err := codec.Encode(putResponse)
			if err != nil {
				fmt.Println("encode put_resp failed, err:", err)
				return
			}
			conn.Write(resp)
			break
		}
	}
}

// etcdapi.Get(); 1 oprand
func etcdGet(msg []byte, conn net.Conn) {
	key := string(msg[1: len(msg) - 1])
	if config.LocalConfig.IsDebug {
		fmt.Println("etcdapi.Get():", key)
	}
	getResponse := etcdapi.Get(key)
	resp, err := codec.Encode([]byte(getResponse))
	if err != nil {
		fmt.Println("encode get_resp failed, err:", err)
		return
	}
	conn.Write(resp)
}

// etcdapi.GetSortedPrefix(); 1 oprand
func etcdGetSortedPrefix(msg []byte, conn net.Conn) {
	key := string(msg[1: len(msg) - 1])
	if config.LocalConfig.IsDebug {
		fmt.Println("etcdapi.GetSortedPrefix():", key)
	}
	getResponse := etcdapi.GetSortedPrefix(key)
	resp, err := codec.Encode(getResponse)
	if err != nil {
		fmt.Println("encode get_resp SortedPrefix failed, err:", err)
		return
	}
	conn.Write(resp)  
}

// etcdapi.GetWithRange(); 2 oprands
func etcdGetWithRange(msg []byte, conn net.Conn) {
	for i, b := range msg[1:] {
		if b == 0x00 {
			key := string(msg[1: i + 1])
			end := string(msg[i + 2: len(msg) - 1])
			if config.LocalConfig.IsDebug {
				fmt.Println("etcdapi.GetWithRange():", key, end)
			}
			getResponse := etcdapi.GetWithRange(key, end)
			resp, err := codec.Encode(getResponse)
			if err != nil {
				fmt.Println("encode get_resp withRange failed, err:", err)
				return
			}
			conn.Write(resp)
			break
		}
	}
}

// etcdapi.GetWithLimit(); 2 oprands
func etcdGetWithLimit(msg []byte, conn net.Conn) {
	for i, b := range msg[1:] {
		if b == 0x00 {
			key := string(msg[1: i + 1])
			var limit int64
			limit = int64(binary.LittleEndian.Uint64(msg[i + 2:]))
			if config.LocalConfig.IsDebug {
				fmt.Println("etcdapi.GetWithLimit():", key, limit)
			}
			getResponse := etcdapi.GetWithLimit(key, limit)
			resp, err := codec.Encode(getResponse)
			if err != nil {
				fmt.Println("encode get_resp withLimit failed, err:", err)
				return
			}
			conn.Write(resp)
			break
		}
	}
}

// etcdapi.Watch(); 1 oprand
func etcdWatch(msg []byte, conn net.Conn) {
	key := string(msg[1: len(msg) - 1])
	if config.LocalConfig.IsDebug {
		fmt.Println("etcdapi.Watch():", key)
	}
	watchResponse := etcdapi.Watch(key)
	for change := range watchResponse {
		resp, err := codec.Encode(change)
		if err != nil {
			fmt.Println("encode get_resp SortedPrefix failed, err:", err)
			return
		}
		fmt.Println("Watch change[]: ", change)
		fmt.Println("Watch resp[]: ", resp)
		conn.Write(resp) 
	}
}