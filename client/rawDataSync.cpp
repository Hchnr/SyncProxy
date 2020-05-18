#include<string.h>

#include "rawDataSync.h"

namespace DataSync {


int RawDataSync::encode(unsigned char op, std::vector<std::string> strs, char* sendbuf) {
    int length = 4;
    sendbuf[length ++] = op;
    for (int i = 0; i < strs.size(); i ++) {
        for (int j = 0; j < strs[i].size(); j ++) {
            sendbuf[length ++] = strs[i][j];
        }
        sendbuf[length ++] = 0;
    }
    *((int*) sendbuf) = length - 4;
    return length;
}


/* Connect to dataSync proxy 
   Return: 0 for OK, 1 for problems
*/
int RawDataSync::init(const char* ip_str, unsigned short port) {
    struct sockaddr_in  servaddr;

    if( (sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        printf("create socket error: %s(errno: %d)\n", strerror(errno),errno);
        return 1;
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    if( inet_pton(AF_INET, ip_str, &servaddr.sin_addr) <= 0){
        printf("inet_pton error for %s\n",ip_str);
        return 1;
    }

    if( connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0){
        printf("connect error: %s(errno: %d)\n",strerror(errno),errno);
        return 0;
    }
    return 0;
}

/* Return: put response */
std::vector<unsigned long long> RawDataSync::etcdPut(std::string key, std::string value){

    char buf[MAXLEN];
    std::vector<unsigned long long> resp;
    std::vector<std::string> strs;
    strs.push_back(key);
    strs.push_back(value);
    int sendlen = encode(0, strs, buf);
    if( send(sockfd, buf, sendlen, 0) < 0){
        printf("send put msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    // (header 4 bytes) +  (4 uint64 LittleEnddian) 
    ssize_t len = recv(sockfd, buf, 36, 0);
    if (len != 36) {
        printf("recv put msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < 4; j ++) {
            unsigned char tmp = buf[4 + i * 8 + j];
            buf[4 + i * 8 + j] = buf[4 + i * 8 + 7 - j];
            buf[4 + i * 8 + 7 - j] = tmp;
        }
    }
    unsigned long long* resp_uint64 = (unsigned long long*) &buf[4];
    for (int i = 0; i < 4; i ++) {
        resp.push_back(resp_uint64[i]);
    }
    return resp;
}

/* Return: get response */
std::string RawDataSync::etcdGet(std::string key){

    char buf[MAXLEN];
    std::vector<std::string> strs;
    strs.push_back(key);
    int sendlen = encode(1, strs, buf);
    if( send(sockfd, buf, sendlen, 0) < 0){
        printf("send get msg error: %s(errno: %d)\n", strerror(errno), errno);
        return "";
    }
    // (header 4 bytes) +  (4 uint64 LittleEnddian) 
    ssize_t len = recv(sockfd, buf, MAXLEN, 0);
    if (len < 0) {
        printf("recv get msg error: %s(errno: %d)\n", strerror(errno), errno);
        return "";
    }
    buf[len] = 0;
    return std::string(buf + 4);
}

/* Return: get sorted prefix response */
std::vector<std::vector<std::string>> RawDataSync::etcdGetSortedPrefix(std::string key){
    std::vector<std::vector<std::string>> resp;
    char buf[MAXLEN];
    std::vector<std::string> strs;
    strs.push_back(key);
    int sendlen = encode(2, strs, buf);
    if( send(sockfd, buf, sendlen, 0) < 0){
        printf("send get SortedPrefix msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    ssize_t len = recv(sockfd, buf, MAXLEN, 0);
    if (len < 0) {
        printf("recv get SortedPrefix msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    char *newstr = buf + 4;
    std::vector<std::string> kvs;
    for (int i = 4; i < len; i ++) {
        if (buf[i] == 0) {
            kvs.push_back(std::string(newstr));
            newstr = buf + i + 1;
        }
    }
    for (int i = 0; i < kvs.size(); i += 2) {
        std::vector<std::string> kv = {kvs[i], kvs[i + 1]};
        resp.push_back(kv);
    }
    return resp;
}

/* WithRange sets the comparison to scan the range [key, end). */
std::vector<std::vector<std::string>> RawDataSync::etcdGetWithRange(std::string key, std::string end){
    std::vector<std::vector<std::string>> resp;
    char buf[MAXLEN];
    std::vector<std::string> strs;
    strs.push_back(key);
    strs.push_back(end);
    int sendlen = encode(3, strs, buf);
    if( send(sockfd, buf, sendlen, 0) < 0){
        printf("send get WithRange msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    ssize_t len = recv(sockfd, buf, MAXLEN, 0);
    if (len < 0) {
        printf("recv get WithRange msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    char *newstr = buf + 4;
    std::vector<std::string> kvs;
    for (int i = 4; i < len; i ++) {
        if (buf[i] == 0) {
            kvs.push_back(std::string(newstr));
            newstr = buf + i + 1;
        }
    }
    for (int i = 0; i < kvs.size(); i += 2) {
        std::vector<std::string> kv = {kvs[i], kvs[i + 1]};
        resp.push_back(kv);
    }
    return resp;
}

/* WithLimit: the number of returned keys is bounded by limit. */
std::vector<std::vector<std::string>> RawDataSync::etcdGetWithLimit(std::string key, long long limit){
    std::vector<std::vector<std::string>> resp;
    char buf[MAXLEN];
    std::vector<std::string> strs;
    strs.push_back(key);
    int sendlen = encode(4, strs, buf);
    *((long long*) (buf + sendlen)) = limit;
    sendlen += 8;
    *((int*) buf) += 8;
    if( send(sockfd, buf, sendlen, 0) < 0){
        printf("send get WithLimit msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    ssize_t len = recv(sockfd, buf, MAXLEN, 0);
    if (len < 0) {
        printf("recv get WithLimit msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    char *newstr = buf + 4;
    std::vector<std::string> kvs;
    for (int i = 4; i < len; i ++) {
        if (buf[i] == 0) {
            kvs.push_back(std::string(newstr));
            newstr = buf + i + 1;
        }
    }
    for (int i = 0; i < kvs.size(); i += 2) {
        std::vector<std::string> kv = {kvs[i], kvs[i + 1]};
        resp.push_back(kv);
    }
    return resp;
}

/* Watch: return changes to kvs. */
std::vector<std::string> RawDataSync::etcdWatch(std::string key){
    std::vector<std::string> resp;
    char buf[MAXLEN];
    std::vector<std::string> strs;
    strs.push_back(key);
    int sendlen = encode(0x08, strs, buf);
    if( send(sockfd, buf, sendlen, 0) < 0){
        printf("send watch msg error: %s(errno: %d)\n", strerror(errno), errno);
        return resp;
    }
    
    while(1) {
        sleep(2);
        if(recv(sockfd, buf, 4, MSG_PEEK) < 0) {
            continue;
        }
        int len = *((int*) buf);
        int readlen = recv(sockfd, buf, len + 4, MSG_PEEK);
        
        if (readlen < 0) {
            printf("recv get msg error: %s(errno: %d)\n", strerror(errno), errno);
            return resp;
        } else if (readlen < len + 4) {
            printf("[info] watch recv: not long enough.\n");
        } else if (readlen == len + 4) {
            recv(sockfd, buf, len + 4, 0);
            if (buf[4] == 0) {
                resp.push_back("put");
            } else if (buf[4] == 1) {
                resp.push_back("delete");
            }
        }
        resp.push_back(std::string(buf + 5));
        for (int i = 6; i < len + 4; i ++) {
            if (buf[i] == 0) {
                resp.push_back(std::string(buf + i + 1));
                printf("Watch: %s %s %s\n", resp[0].c_str(), resp[1].c_str(), resp[2].c_str());
                resp.clear();
                break;
            }
        }
    }
    return resp;
}

std::string RawDataSync::redisSet(std::string key, std::string value) {
    char buf[MAXLEN];
    std::vector<unsigned long long> resp;
    std::vector<std::string> strs;
    strs.push_back(key);
    strs.push_back(value);
    int sendlen = encode(0x10, strs, buf);
    if( send(sockfd, buf, sendlen, 0) < 0){
        printf("send redis set msg error: %s(errno: %d)\n", strerror(errno), errno);
        return "";
    }
    // (header 4 bytes) +  (4 uint64 LittleEnddian) 
    ssize_t len = recv(sockfd, buf, MAXLEN, 0);
    if (len < 0) {
        printf("recv redis set msg error: %s(errno: %d)\n", strerror(errno), errno);
        return "";
    }
    buf[len] = 0;
    return std::string(buf + 4);
}

std::string RawDataSync::redisGet(std::string key) {
    char buf[MAXLEN];
    std::vector<std::string> strs;
    strs.push_back(key);
    int sendlen = encode(0x11, strs, buf);
    if( send(sockfd, buf, sendlen, 0) < 0){
        printf("send get msg error: %s(errno: %d)\n", strerror(errno), errno);
        return "";
    }
    // (header 4 bytes) +  (4 uint64 LittleEnddian) 
    ssize_t len = recv(sockfd, buf, MAXLEN, 0);
    if (len < 0) {
        printf("recv get msg error: %s(errno: %d)\n", strerror(errno), errno);
        return "";
    }
    buf[len] = 0;
    return std::string(buf + 4);
}


} // namespace DataSync