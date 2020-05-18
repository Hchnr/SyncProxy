#ifndef DATASYNC_H
#define DATASYNC_H

#include "rawDataSync.h"

namespace DataSync {

class DataSync {
private:
    RawDataSync* proxy;

public:
    DataSync(const char* ip_str, unsigned short port) {
        proxy = new RawDataSync(ip_str, port);
    }

    ~DataSync() {
        delete proxy;
    }

    /* Return: put response */
    int Set(std::string key, std::string value, int cluster);

    /* Return: get response */
    std::string Get(std::string key, int cluster);

    /* Watch: client need to rewrite this method. */
    void Watch(std::string key, int cluster); 

    /* Return: get sorted prefix response */
    std::vector<std::vector<std::string>> GetSortedPrefix(std::string key, int cluster);

    /* WithRange sets the comparison to scan the range [key, end). */
    std::vector<std::vector<std::string>> GetWithRange(std::string key, std::string end, int cluster);

    /* WithLimit: the number of returned keys is bounded by limit. */
    std::vector<std::vector<std::string>> GetWithLimit(std::string key, long long limit, int cluster); 


}; // clase DataSync

} // namespace DataSync

#endif // DATASYNC_H