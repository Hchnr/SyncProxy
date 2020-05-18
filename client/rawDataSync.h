#ifndef RAWDATASYNC_H
#define RAWDATASYNC_H

#include<stdio.h>
#include<stdlib.h>
#include<errno.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<unistd.h>
#include<vector>
#include<string>

namespace DataSync {

class RawDataSync {
private:
    int   sockfd;
    const int MAXLEN = 65536;

public:
    RawDataSync(const char* ip_str, unsigned short port) {
        init(ip_str, port);
    }

    ~RawDataSync() {
        close(sockfd);
    }

    /* Connect to dataSync proxy 
    Return: 0 for OK, 1 for problems
    */
    int init(const char* ip_str, unsigned short port);

    /* Return: put response */
    std::vector<unsigned long long> etcdPut(std::string key, std::string value);

    /* Return: get response */
    std::string etcdGet(std::string key);

    /* Return: get sorted prefix response */
    std::vector<std::vector<std::string>> etcdGetSortedPrefix(std::string key);

    /* WithRange sets the comparison to scan the range [key, end). */
    std::vector<std::vector<std::string>> etcdGetWithRange(std::string key, std::string end);

    /* WithLimit: the number of returned keys is bounded by limit. */
    std::vector<std::vector<std::string>> etcdGetWithLimit(std::string key, long long limit); 

    /* Watch: return changes to kvs. */
    std::vector<std::string> etcdWatch(std::string key); 
            
    std::string redisSet(std::string key, std::string value);

    std::string redisGet(std::string key);

private:
    /* Params:
        op: 
            0x00 Put in etcd; 
            0x01 Get in etcd;
            0x10 Put in redis; 
            0x11 Get in redis;
        strs:
            oprands for Get or Put
        sendbuf:
            dest of encoding
        return:
            the length in sendbuf
    */
    int encode(unsigned char op, std::vector<std::string> strs, char* sendbuf);

}; // clase RawDataSync

} // namespace DataSync

#endif // RAWDATASYNC_H