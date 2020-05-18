package main
 
import (
    "bufio"
    "fmt"
	"net"
    "io"
    "strconv"
    "github.com/hchnr/etcd-proxy/codec"
    "github.com/hchnr/etcd-proxy/config"
    "github.com/hchnr/etcd-proxy/redisapi"
)

func process(conn net.Conn) {
    defer conn.Close()
    reader := bufio.NewReader(conn)
    for {
        msg, err := codec.Decode(reader)
        if err == io.EOF {
            return
        }
        if err != nil {
            fmt.Println("decode msg failed, err:", err)
            return
        }
        if config.LocalConfig.IsDebug {
            fmt.Println("收到client发来的数据：", msg)
        }
        switch msg[0] {
        case 0x00:
            // etcdapi.Put(); 2 oprands
            go etcdPut(msg[:], conn)
        case 0x01:
            // etcdapi.Get(); 1 oprand
            go etcdGet(msg[:], conn)
        case 0x02:
            // etcdapi.GetSortedPrefix(); 1 oprand
            go etcdGetSortedPrefix(msg[:], conn)
        case 0x03:
            // etcdapi.GetWithRange(); 2 oprands
            go etcdGetWithRange(msg[:], conn)
        case 0x04:
            // etcdapi.GetWithLimit(); 2 oprands
            go etcdGetWithLimit(msg[:], conn)
        case 0x08:
            // etcdapi.Watch(); 1 oprand
            go etcdWatch(msg[:], conn)
        case 0x10:
            // redisapi.Set(); 2 oprands
            go redisSet(msg[:], conn)
        case 0x11:
            // redisapi.Get(); 1 oprand
            go redisGet(msg[:], conn)
        }
    }
}

func main() {
    ip := config.LocalConfig.ServeIP
    port := strconv.FormatInt(int64(config.LocalConfig.ServePort), 10)
    listen, err := net.Listen("tcp", ip + ":" + port)
    if err != nil {
        fmt.Println("listen failed, err:", err)
        return
    }
    defer listen.Close()
    for {
        conn, err := listen.Accept()
        if err != nil {
            fmt.Println("accept failed, err:", err)
            continue
        }
        go process(conn)
    }
}

// SET and the GET a key to check Redis
func redisAPITest() {
    redisapi.Set("name", "hechenrui")
    redisapi.Flush()
    redisapi.Recv()
    redisapi.Get("name")
    redisapi.Flush()
    redisapi.Recv()
}
