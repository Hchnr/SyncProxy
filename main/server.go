package main
 
import (
    "bufio"
    "encoding/binary"
    "fmt"
	"net"
    "io"
    "github.com/hchnr/etcd-proxy/codec"
    "github.com/hchnr/etcd-proxy/etcdapi"
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
        fmt.Println("收到client发来的数据：", msg)
        switch msg[0] {
        case 0x00:
            // etcdapi.Put(); 2 oprands
            for i, b := range msg[1:] {
                if b == 0x00 {
                    key := string(msg[1: i + 1])
                    value := string(msg[i + 2: len(msg) - 1])
                    fmt.Println("etcdapi.Put():", key, value)
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
            
        case 0x01:
            // etcdapi.Get(); 1 oprand
            key := string(msg[1: len(msg) - 1])
            fmt.Println("etcdapi.Get():", key)
            getResponse := etcdapi.Get(key)
            resp, err := codec.Encode([]byte(getResponse))
            if err != nil {
                fmt.Println("encode get_resp failed, err:", err)
                return
            }
            conn.Write(resp)
        case 0x02:
            // etcdapi.GetSortedPrefix(); 1 oprand
            key := string(msg[1: len(msg) - 1])
            fmt.Println("etcdapi.GetSortedPrefix():", key)
            getResponse := etcdapi.GetSortedPrefix(key)
            resp, err := codec.Encode(getResponse)
            if err != nil {
                fmt.Println("encode get_resp SortedPrefix failed, err:", err)
                return
            }
            conn.Write(resp)    
        case 0x03:
            // etcdapi.GetWithRange(); 2 oprands
            for i, b := range msg[1:] {
                if b == 0x00 {
                    key := string(msg[1: i + 1])
                    end := string(msg[i + 2: len(msg) - 1])
                    fmt.Println("etcdapi.GetWithRange():", key, end)
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
        case 0x04:
            // etcdapi.GetWithLimit(); 2 oprands
            for i, b := range msg[1:] {
                if b == 0x00 {
                    key := string(msg[1: i + 1])
                    var limit int64
                    limit = int64(binary.LittleEndian.Uint64(msg[i + 2:]))
                    fmt.Println("etcdapi.GetWithLimit():", key, limit)
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
        case 0x08:
            // etcdapi.Watch(); 1 oprand
            key := string(msg[1: len(msg) - 1])
            fmt.Println("etcdapi.Watch():", key)
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
              
        case 0x10:
            // redisapi.Put();
        case 0x11:
            // redisapi.Get();
        }
    }
}

func main() {

    listen, err := net.Listen("tcp", "127.0.0.1:30000")
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

