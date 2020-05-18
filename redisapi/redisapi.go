package redisapi

import (
    "fmt"
    "github.com/gomodule/redigo/redis"
    "log"
    "strconv"

    "github.com/hchnr/etcd-proxy/config"
)

// redisConn represents a connection to a Redis server.
var redisConns []*redis.Conn
var redisConnsAvail chan *redis.Conn

func init() {
    wokersNum := config.LocalConfig.RedisProxy.WorkkersNum
    fmt.Printf("%d Redis client initialized.\n", wokersNum)
    redisConns = make([]*redis.Conn, wokersNum, wokersNum)
    redisConnsAvail = make(chan *redis.Conn, wokersNum)
    
    ip := config.LocalConfig.RedisCluster.Servers[0].IP
	port := strconv.FormatInt(int64(config.LocalConfig.RedisCluster.Servers[0].Port), 10)
    url := "redis://" + ip + ":" + port
    
    for i := 0; i < wokersNum; i ++ {
        conn, err := redis.DialURL(url)
        if err != nil {
            log.Fatalln(err)
        }
        redisConns[i] = &conn
        redisConnsAvail <- &conn
        // client closed after program exit
        // defer conn.Close()
    }
}

// Put Get-Request in request-buffer
func Get(key string) (reply interface{}, err error) {
    redisConn := <-redisConnsAvail
    err = (*redisConn).Send("GET", key)
    if err != nil {
        fmt.Println("redis get send error", err)
        return nil, err
    }
    err = (*redisConn).Flush()
    if err != nil {
        fmt.Println("redis get flush error", err)
        return nil, err
    }
    reply, err = (*redisConn).Receive()
    redisConnsAvail <- redisConn
    return reply, err
}

// Put Set-Request in request-buffer
func Set(key string, value string) (reply interface{}, err error) {
    redisConn := <-redisConnsAvail
    err = (*redisConn).Send("SET", key, value)
    if err != nil {
        fmt.Println("redis send error", err)
        return nil, err
    }
    err = (*redisConn).Flush()
    if err != nil {
        fmt.Println("redis flush error", err)
        return nil, err
    }
    reply, err = (*redisConn).Receive()
    redisConnsAvail <- redisConn
    return reply, err
}

// Flush flushes the output buffer to the Redis server.
func Flush() error {
    redisConn := <-redisConnsAvail
    err := (*redisConn).Flush()
    redisConnsAvail <- redisConn
    return err
}

// Receive receives a single reply from the Redis server
func Recv() (reply interface{}, err error) {
    redisConn := <-redisConnsAvail
    reply, err = (*redisConn).Receive()
    redisConnsAvail <- redisConn
    if err != nil {
        fmt.Println("Redis Recv() failed, err:", err)
        return
    }
    return reply, err
}

// Close closes the connection.
func Close() error {
    redisConn := <-redisConnsAvail
    err := (*redisConn).Close()
    redisConnsAvail <- redisConn
    return err
}

// Chech type for Recv(), it returns interface{}
func CheckType(args interface{}) {
    switch args.(type) {  
        case int:  
            fmt.Println("CheckType: int value.")  
        case string:  
            fmt.Println("CheckType: string value.")  
        case int64:  
            fmt.Println("CheckType: int64 value.")  
        case []byte:  
            fmt.Println("CheckType: []byte value.")  
        case []string:  
            fmt.Println("CheckType: []string value.")  
        case [][]byte:  
            fmt.Println("CheckType: [][]byte value.")  
        default:  
            fmt.Println("CheckType: unknown type.")  
    }
}