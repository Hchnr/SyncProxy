#include <sys/time.h>
#include <pthread.h>

#include "rawDataSync.h"

void testEtcd();
void testRedis(); 
void testSingleConn();
void testMultiConnThroughput();
void testMultiConnDelay();

const std::string ip = "172.17.0.2";
const int THREAD_NUM = 2;
const int REQUEST_NUM = 5000;

int main(int argc, char** argv){

    testEtcd();
    testRedis();
    // testSingleConn();
    // testMultiConnThroughput();
    testMultiConnDelay();
    return 0;
}

void* thread_func_delay(void* arg) {
    double* time_use = new double(0);
    struct timeval start;
    struct timeval end;

    DataSync::RawDataSync *dataSync = new DataSync::RawDataSync("162.105.85.63", 32383);
    std::string value256;
    for (int i = 0; i < 256; i ++) {
        value256.push_back('0' + (i % 10));
    }
    
    for (int i = 0; i < REQUEST_NUM; i ++) {
        gettimeofday(&start, NULL); 
        // auto putResp = dataSync->etcdPut("cppDemo", value256);
        std::string setResp = dataSync->redisSet("redisTes", value256);
        gettimeofday(&end, NULL);
        *time_use += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    }
    
    *time_use /= REQUEST_NUM;
    // printf("time_use is %.10f\n", *time_use);
    return time_use;
}

void testMultiConnDelay() {
    pthread_t tid[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; i ++) {
        if (pthread_create(tid + i, NULL, thread_func_delay, NULL) != 0) {
            printf("pthread_create error.");
            exit(EXIT_FAILURE);
        }
    }

    double* rev = NULL;
    double avg = 0;
    std::vector<double> times;
    for (int i = 0; i < THREAD_NUM; i ++) {
        pthread_join(tid[i], (void **)&rev);
        times.push_back(*rev);
        avg += *rev;
        delete rev;
    }
    avg /= THREAD_NUM;
    printf("avg delay(ms): %f\n", avg * 1000);
}

void* thread_func_throughput(void* arg) {
    double* time_use = new double(0);
    struct timeval start;
    struct timeval end;

    DataSync::RawDataSync *dataSync = new DataSync::RawDataSync("162.105.85.63", 32383);
    std::string value256;
    for (int i = 0; i < 256; i ++) {
        value256.push_back('0' + (i % 10));
    }
    gettimeofday(&start, NULL); 
    for (int i = 0; i < REQUEST_NUM; i ++) {
        // auto putResp = dataSync->etcdPut("cppDemo", value256);
        std::string setResp = dataSync->redisSet("redisTes", value256);
    }
    gettimeofday(&end, NULL);
    *time_use=(end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    // printf("time_use is %.10f\n", *time_use);
    return time_use;
}

void testMultiConnThroughput() {
    pthread_t tid[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; i ++) {
        if (pthread_create(tid + i, NULL, thread_func_throughput, NULL) != 0) {
            printf("pthread_create error.");
            exit(EXIT_FAILURE);
        }
    }

    double* rev = NULL;
    double avg = 0;
    std::vector<double> times;
    for (int i = 0; i < THREAD_NUM; i ++) {
        pthread_join(tid[i], (void **)&rev);
        times.push_back(*rev);
        avg += *rev;
        delete rev;
    }
    avg /= THREAD_NUM;
    printf("avg time: %f\n", avg);
    printf("throughput: %f\n", REQUEST_NUM * THREAD_NUM / avg);
}

void testSingleConn() {
    double time_use = 0;
    struct timeval start;
    struct timeval end;
    
    printf("\nTest 1k requests in etcd.\n");
    DataSync::RawDataSync *dataSync = new DataSync::RawDataSync("162.105.85.63", 32383);

    gettimeofday(&start, NULL); 
    for (int i = 0; i < 1000; i ++) {
        auto putResp = dataSync->etcdPut("cppDemo", "20200405");
    }
    gettimeofday(&end, NULL);
    
    printf("start.tv_sec:%d\n",start.tv_sec);
    printf("start.tv_usec:%d\n",start.tv_usec);
    printf("end.tv_sec:%d\n",end.tv_sec);
    printf("end.tv_usec:%d\n",end.tv_usec);

    time_use=(end.tv_sec-start.tv_sec)*1000000+(end.tv_usec-start.tv_usec);
    printf("time_use is %.10f\n",time_use/1000000);

}

void testRedis() {
    DataSync::RawDataSync *dataSync = new DataSync::RawDataSync("162.105.85.63", 32383);
    // DataSync::RawDataSync *dataSync = new DataSync::RawDataSync("127.0.0.1", 30000);

    // 1. redis.set()
    std::string setResp = dataSync->redisSet("redisTest", "20200426");
    printf("redisSet <redisTest, 20200426> in redis\n");
    printf("redisSet resp: %s\n", setResp.c_str());
    
    // 2. redis.get()
    std::string getResp = dataSync->redisGet("redisTest");
    printf("redisGet <redisTest, 20200426> in redis\n");
    printf("redisGet resp: %s\n", getResp.c_str());
}

void testEtcd() {
    DataSync::RawDataSync *dataSync = new DataSync::RawDataSync("162.105.85.63", 32383);
    // DataSync::RawDataSync *dataSync = new DataSync::RawDataSync("127.0.0.1", 30000);
    
    // 1. etcd.put()
    std::vector<unsigned long long> putResp = dataSync->etcdPut("cppDemo", "20200405");
    printf("etcdPut <cppDemo, 20200405> in etcd\n");
    printf("etcdPut resp: %llu %llu %llu %llu\n", putResp[0], putResp[1], putResp[2], putResp[3]);
    dataSync->etcdPut("cppDemo2", "20200406");
    dataSync->etcdPut("cppDemo3", "20200407");

    // 2. etcd.get()
    std::string getResp = dataSync->etcdGet("cppDemo");
    printf("etcdGet <cppDemo> in etcd\n");
    printf("etcdGet resp: %s\n", getResp.c_str());

    // 3. etcd.getSortedPrefix()
    std::vector<std::vector<std::string>> getSortedPrefixResp = dataSync->etcdGetSortedPrefix("cppDemo");
    printf("etcdGetSortedPrefix <cppDemo> in etcd\n");
    for (auto kv : getSortedPrefixResp) {
        printf("etcdGetSortedPrefix resp: %s : %s\n", kv[0].c_str(), kv[1].c_str());
    }

    // 4. etcd.WithRange()
    std::vector<std::vector<std::string>> getWithRangeResp = dataSync->etcdGetWithRange("cppDemo", "cppDemo3");
    printf("etcdGetWithRange <cppDemo, cppDemo3> in etcd\n");
    for (auto kv : getWithRangeResp) {
        printf("etcdGetWithRange resp: %s : %s\n", kv[0].c_str(), kv[1].c_str());
    }

    // 5. etcd.WithLimit()
    std::vector<std::vector<std::string>> getWithLimitResp = dataSync->etcdGetWithLimit("cppDemo", 2);
    printf("etcdGetWithLimit <cppDemo, 2> in etcd\n");
    for (auto kv : getWithLimitResp) {
        printf("etcdGetWithLimit resp: %s : %s\n", kv[0].c_str(), kv[1].c_str());
    }

    // 6. etcd.Watch()
    // Then change the key from other clients.
    /*
    printf("etcdWatch <cppDemo> in etcd beginning.\n");
    dataSync->etcdWatch("cppDemo");
    printf("etcdWatch <cppDemo> in etcd finished.\n");
    */
}
