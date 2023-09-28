#include <thread>
#include <atomic>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <cstring>
#include <mutex>

#include "../include/lockwrapper.h"
#include "zmalloc.h"
#include "util.h"
#include "gallocator.h"

#define DEBUG_LEVEL LOG_WARNING
#define STEPS 204800 //100M much larger than 10M L3 cache
#define DEBUG_LEVEL LOG_WARNING

#define SYNC_KEY STEPS

int node_id;

int is_master = 1;
string ip_master = get_local_ip("eth0");
string ip_worker = get_local_ip("eth0");
int port_master = 12345;
int port_worker = 12346;

const char* result_file = "result.csv";

//exp parameters
long ITERATION = 2000000;
//long FENCE_PERIOD = 1000;
int no_thread = 2;
int no_node = 1;
int remote_ratio = 0;  //0..100
int shared_ratio = 10;  //0..100
int space_locality = 10;  //0..100
int time_locality = 10;  //0..100 (how probable it is to re-visit the current position)
int read_ratio = 10;  //0..100
int op_type = 0;  //0: read/write; 1: rlock/wlock; 2: rlock+read/wlock+write

float cache_th = 0.15;  //0.15
uint64_t allocated_mem_size = 0;
int compute_num = 100;
int memory_num = 100;
//runtime statistics
atomic<long> remote_access(0);
atomic<long> shared_access(0);
atomic<long> space_local_access(0);
atomic<long> time_local_access(0);
atomic<long> read_access(0);

atomic<long> total_throughput(0);
atomic<long> avg_latency(0);

bool reset = false;

set<GAddr> gen_accesses;
set<GAddr> real_accesses;
LockWrapper stat_lock;


int addr_size = sizeof(GAddr);
int item_size = addr_size;
int items_per_block = BLOCK_SIZE / item_size;

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--ip_master") == 0) {
            ip_master = string(argv[++i]);
        } else if (strcmp(argv[i], "--ip_worker") == 0) {
            ip_worker = string(argv[++i]);
        } else if (strcmp(argv[i], "--port_master") == 0) {
            port_master = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--iface_master") == 0) {
            ip_master = get_local_ip(argv[++i]);
        } else if (strcmp(argv[i], "--port_worker") == 0) {
            port_worker = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--iface_worker") == 0) {
            ip_worker = get_local_ip(argv[++i]);
        } else if (strcmp(argv[i], "--iface") == 0) {
            ip_worker = get_local_ip(argv[++i]);
            ip_master = get_local_ip(argv[i]);
        } else if (strcmp(argv[i], "--is_master") == 0) {
            is_master = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--no_thread") == 0) {
            no_thread = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--remote_ratio") == 0) {
            remote_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--shared_ratio") == 0) {
            shared_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--read_ratio") == 0) {
            read_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--space_locality") == 0) {
            space_locality = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--time_locality") == 0) {
            time_locality = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--op_type") == 0) {
            op_type = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--no_node") == 0) {
            no_node = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--result_file") == 0) {
            result_file = argv[++i];  //0..100
        } else if (strcmp(argv[i], "--item_size") == 0) {
            item_size = atoi(argv[++i]);
            items_per_block = BLOCK_SIZE / item_size;
        } else if (strcmp(argv[i], "--allocated_mem_size") == 0) {
            allocated_mem_size = atoi(argv[++i]);
            allocated_mem_size = allocated_mem_size*1024ull*1024*1024;
        } else if (strcmp(argv[i], "--compute_num") == 0) {
            compute_num = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--memory_num") == 0) {
            memory_num = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--cache_th") == 0) {
            cache_th = atof(argv[++i]);
        } else {
            fprintf(stderr, "Unrecognized option %s for benchmark\n", argv[i]);
        }
    }

#ifdef LOCAL_MEMORY
    int memory_type = 0;  //"local memory";
#else
    int memory_type = 1;  //"global memory";
#endif
    printf("Currently configuration is: ");
    printf(
            "master: %s:%d, worker: %s:%d, is_master: %s, no_thread: %d, no_node: %d\n",
            ip_master.c_str(), port_master, ip_worker.c_str(), port_worker,
            is_master == 1 ? "true" : "false", no_thread, no_node);
    printf(
            "no_node = %d, no_thread = %d, remote_ratio: %d, shared_ratio: %d, read_ratio: %d, "
            "space_locality: %d, time_locality: %d, op_type = %s, memory_type = %s, item_size = %d, cache_th = %f, result_file = %s\n",
            no_node,
            no_thread,
            remote_ratio,
            shared_ratio,
            read_ratio,
            space_locality,
            time_locality,
            op_type == 0 ?
            "read/write" :
            (op_type == 1 ?
             "rlock/wlock" :
             (op_type == 2 ? "rlock+read/wlock+write" : "try_rlock/try_wlock")),
            memory_type == 0 ? "local memory" : "global memory", item_size, cache_th,
            result_file);

    //srand(1);

    Conf conf;
    conf.loglevel = DEBUG_LEVEL;
    conf.is_master = is_master;
    conf.master_ip = ip_master;
    conf.master_port = port_master;
    conf.worker_ip = ip_worker;
    conf.worker_port = port_worker;
    conf.size = allocated_mem_size;
    long size = ((long) BLOCK_SIZE) * STEPS * no_thread * 4;
    conf.size = size < conf.size ? conf.size : size;
//    conf.cache_size = cache_size;
//    conf.cache_size = 0;
    conf.cache_th = 0;
    GAlloc* alloc = GAllocFactory::CreateAllocator(&conf);
    int id;
    node_id = alloc->GetID();
    no_node = compute_num + memory_num;
    printf("This node id is %d\n", node_id);
    alloc->Put(SYNC_KEY + node_id, &node_id, sizeof(int));
    for (int i = 1; i <= no_node; i++) {
        alloc->Get(SYNC_KEY + i, &id);
        epicAssert(id == i);
    }
    long res[3];
    for (int i = 1; i <= compute_num; i++) {
        alloc->Get(SYNC_KEY + no_node + i, &res);
    }
    printf("Memory ndoe %d quit\n", node_id);
  return 0;
}