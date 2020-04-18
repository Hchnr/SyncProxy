#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<unistd.h>
#include<vector>
#include<string>

#include "dataSync.hpp"

int main(int argc, char** argv){
    DataSync::DataSync *dataSync = new DataSync::DataSync("127.0.0.1", 30000);
    // 1. etcd.put()
    std::vector<unsigned long long> putResp = dataSync->put("cppDemo", "20200405");
    printf("put <cppDemo, 20200405> in etcd\n");
    printf("put resp: %llu %llu %llu %llu\n", putResp[0], putResp[1], putResp[2], putResp[3]);
    dataSync->put("cppDemo2", "20200406");
    dataSync->put("cppDemo3", "20200407");
    // 2. etcd.get()
    std::string getResp = dataSync->get("cppDemo");
    printf("get <cppDemo> in etcd\n");
    printf("get resp: %s\n", getResp.c_str());
    // 3. etcd.getSortedPrefix()
    std::vector<std::vector<std::string>> getSortedPrefixResp = dataSync->getSortedPrefix("cppDemo");
    printf("getSortedPrefix <cppDemo> in etcd\n");
    for (auto kv : getSortedPrefixResp) {
        printf("getSortedPrefix resp: %s : %s\n", kv[0].c_str(), kv[1].c_str());
    }
    // 4. etcd.WithRange()
    std::vector<std::vector<std::string>> getWithRangeResp = dataSync->getWithRange("cppDemo", "cppDemo3");
    printf("getWithRange <cppDemo, cppDemo3> in etcd\n");
    for (auto kv : getWithRangeResp) {
        printf("getWithRange resp: %s : %s\n", kv[0].c_str(), kv[1].c_str());
    }
    // 5. etcd.WithLimit()
    std::vector<std::vector<std::string>> getWithLimitResp = dataSync->getWithLimit("cppDemo", 2);
    printf("getWithLimit <cppDemo, 2> in etcd\n");
    for (auto kv : getWithLimitResp) {
        printf("getWithLimit resp: %s : %s\n", kv[0].c_str(), kv[1].c_str());
    }
    // 6. etcd.Watch()
    printf("Watch <cppDemo> in etcd beginning.\n");
    dataSync->Watch("cppDemo");
    printf("Watch <cppDemo> in etcd finished.\n");
    return 0;
}

