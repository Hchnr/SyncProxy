package config

import(
    "io/ioutil"
    "encoding/json"
    "fmt"
    "runtime"
)

func init(){
    // Catch errors when parsing
    defer func() {
        if err := recover(); err != nil{
            switch err.(type) {
                case runtime.Error: 
                    fmt.Println("runtime error:", err)
                default: 
                    fmt.Println("error:", err)
            }
        }
    }()
    if err := LoadJsonData("../config/config.json", LocalConfig); err != nil{
        fmt.Println("Loading config.json failed.")
        panic(err)
    }
}

type Server struct{
    IP string `json:"ip"`
    Port int16 `json:"port"`
}

type Cluster struct{
    Servers []Server `json:"servers"`
}

type Proxy struct{
    WorkkersNum int `json:"workers_num"`
    DialTimeout int `json:"dial_timeout"`
    RespSize int    `json:"resp_size"`

}

type Config struct {
    ServeIP string `json:"serve_ip"`
    ServePort int16 `json:"serve_port"`
    IsDebug bool `json:"is_debug"`
    EtcdCluster Cluster `json:"etcd_cluster"`
    RedisCluster Cluster `json:"redis_cluster"`
    EtcdProxy Proxy `json:"etcd_proxy"`
    RedisProxy Proxy `json:"redis_proxy"`
}

var LocalConfig = &Config{}

/*
   @msg: read .json file, parse into struct v
   @param: fileName, JsonStruct
   @return: error
*/
func LoadJsonData(fileName string, v interface{}) error{
    data, err := ioutil.ReadFile(fileName)
    if err != nil{
        fmt.Println("Open config.json failed.")
        return err;
    }

    dataJson := []byte(data)

    if err = json.Unmarshal(dataJson, v); err != nil{
        fmt.Println("Parsing config.json failed.")
        return err
    }
    return nil
}


