package main
 
import (
    "fmt"
	"net"
	"github.com/hchnr/etcd-proxy/codec"
	"github.com/hchnr/etcd-proxy/config"
    "github.com/hchnr/etcd-proxy/redisapi"
)

// redisapi.Set(); 2 oprands
func redisSet(msg []byte, conn net.Conn) {
	for i, b := range msg[1:] {
		if b == 0x00 {
			key := string(msg[1: i + 1])
			value := string(msg[i + 2: len(msg) - 1])
			if config.LocalConfig.IsDebug {
				fmt.Println("redisapi.Put():", key, value)
			}
			/*  Response:
				OK, not OK
			*/ 
			setResponse, err := redisapi.Set(key, value)
			if err != nil {
				fmt.Println("Redis set failed, err:", err)
				return
			}
			if config.LocalConfig.IsDebug {
				fmt.Println("redisapi.Put() resp:", setResponse)
			}
			var respString string
			if v, ok := setResponse.(string); ok {
				respString = v
			} else {
				respString = "error"
			}
			resp, err := codec.Encode([]byte(respString))
			if err != nil {
				fmt.Println("encode setResponse failed, err:", err)
				return
			}
			conn.Write(resp)
			break
		}
	}
}

// redisapi.Get(); 1 oprand
func redisGet(msg []byte, conn net.Conn) {
	key := string(msg[1: len(msg) - 1])
	if config.LocalConfig.IsDebug {
		fmt.Println("redisapi.Get():", key)
	}
	getResponse, err := redisapi.Get(key)
	if err != nil {
		fmt.Println("Redis get failed, err:", err)
		return
	}
	var respBytes []byte
	if v, ok := getResponse.(string); ok {
		respBytes = []byte(v)
	} else if v, ok := getResponse.([]byte); ok {
		respBytes = v
	} else {
		respBytes = []byte("")
	}
	// fmt.Println("redisapi.Get() []byte resp:", respBytes)
	resp, err := codec.Encode(respBytes)
	// fmt.Println("redisapi.Get() encoded resp:", resp)
	if err != nil {
		fmt.Println("encode respBytes failed, err:", err)
		return
	}
	conn.Write(resp)
}
