package main
 
import (
    "fmt"
    "net"
    "github.com/hchnr/etcd-proxy/codec"
)

func main() {
    conn, err := net.Dial("tcp", "127.0.0.1:30000")
    if err != nil {
        fmt.Println("dial failed, err", err)
        return
    }
    defer conn.Close()
    // Put <keyFromDemo, valueTest> in etcd
    for i := 0; i < 1; i++ {
        key := append([]byte("keyFromGolangDemo"), 0)
        value := append([]byte("valueTest"), 0)
        msg := append(append([]byte{0}, key...), value...)
        data, err := codec.Encode(msg)
        if err != nil {
            fmt.Println("encode msg failed, err:", err)
            return
        }
        conn.Write(data)
    }
}