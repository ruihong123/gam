// Copyright (c) 2018 The GAM Authors 


#include <stdio.h>
#include "gallocator.h"
#include "workrequest.h"
#include "zmalloc.h"
#include <cstring>
#include "../include/lockwrapper.h"

const Conf* GAllocFactory::conf = nullptr;
Worker* GAllocFactory::worker;
Master* GAllocFactory::master;
LockWrapper GAllocFactory::lock;
#ifdef GFUNC_SUPPORT
GFunc* GAllocFactory::gfuncs[] = { Incr, IncrDouble, GatherPagerank,
    ApplyPagerank, ScatterPagerank };
#endif
std::string trim(const std::string &s) {
    std::string res = s;
    if (!res.empty()) {
        res.erase(0, res.find_first_not_of(" "));
        res.erase(res.find_last_not_of(" ") + 1);
    }
    return res;
}
GAlloc::GAlloc(Worker* worker)
    : wh(new WorkerHandle(worker)) {
    if (!connectMemcached()) {
        printf("Failed to connect to memcached\n");
        return;
    }
    char temp[100] = "Try me ahahahahaha! kkk";
    memSet(reinterpret_cast<const char *>(&temp), 100, reinterpret_cast<const char *>(&temp), 100);
}

GAddr GAlloc::Malloc(const Size size, Flag flag) {
  return Malloc(size, Gnullptr, flag);
}
GAddr GAlloc::Malloc(const Size size, GAddr base, Flag flag) {
#ifdef LOCAL_MEMORY_HOOK
  void* laddr = zmalloc(size);
  return (GAddr)laddr;
#else
  WorkRequest wr = { };
  wr.op = MALLOC;
  wr.flag = flag;
  wr.size = size;

  if (base) {
    wr.addr = base;
  }

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "malloc failed");
    return Gnullptr;
  } else {
    epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
    return wr.addr;
  }
#endif
}

GAddr GAlloc::AlignedMalloc(const Size size, Flag flag) {
  return AlignedMalloc(size, Gnullptr, flag);
}
GAddr GAlloc::AlignedMalloc(const Size size, GAddr base, Flag flag) {
#ifdef LOCAL_MEMORY_HOOK
  void* ret;
  int rret = posix_memalign(&ret, BLOCK_SIZE, size);
  epicAssert(!rret && (GAddr)ret % BLOCK_SIZE == 0);
  return (GAddr)ret;
#else
  WorkRequest wr = { };
  wr.op = MALLOC;
  wr.flag = flag;
  wr.flag |= ALIGNED;
  wr.size = size;

  if (base) {
    wr.addr = base;
  }

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "malloc failed");
    return Gnullptr;
  } else {
    epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
    epicAssert(wr.addr % BLOCK_SIZE == 0);
    return wr.addr;
  }
#endif
}

GAddr GAlloc::Calloc(Size nmemb, Size size, Flag flag, GAddr base) {
  epicLog(LOG_WARNING, "not supported for now");
  return Gnullptr;
}

GAddr GAlloc::Realloc(GAddr ptr, Size size, Flag flag) {
  epicLog(LOG_WARNING, "not supported for now");
  return Gnullptr;
}

void GAlloc::Free(const GAddr addr) {
  WorkRequest wr = { };
  wr.op = FREE;
  wr.addr = addr;

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "free failed");
  } else {
    epicLog(LOG_DEBUG, "free %x:%lx succeeded!", WID(wr.addr), OFF(wr.addr));
  }
}

int GAlloc::Read(const GAddr addr, void* buf, const Size count, Flag flag) {
  return Read(addr, 0, buf, count, flag);
}
int GAlloc::Read(const GAddr addr, const Size offset, void* buf,
                 const Size count, Flag flag) {
#ifdef LOCAL_MEMORY_HOOK
  char* laddr = (char*)addr;
  memcpy(buf, laddr+offset, count);
  return count;
#else
  WorkRequest wr { };
  wr.op = READ;
  wr.flag = flag;
  wr.size = count;
  wr.addr = GADD(addr, offset);
  wr.ptr = buf;

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "read failed");
    return 0;
  } else {
    return wr.size;
  }
#endif
}

#ifdef GFUNC_SUPPORT
int GAlloc::Write(const GAddr addr, void* buf, const Size count, GFunc* func,
                  uint64_t arg, Flag flag) {
  return Write(addr, 0, buf, count, flag, func, arg);
}
#endif

int GAlloc::Write(const GAddr addr, void* buf, const Size count, Flag flag) {
  return Write(addr, 0, buf, count, flag);
}

#ifdef GFUNC_SUPPORT
int GAlloc::Write(const GAddr addr, const Size offset, void* buf,
                  const Size count, Flag flag, GFunc* func, uint64_t arg) {
#else
  int GAlloc::Write(const GAddr addr, const Size offset, void* buf, const Size count, Flag flag) {
#endif
#ifdef LOCAL_MEMORY_HOOK
  char* laddr = (char*)addr;
  memcpy(laddr+offset, buf, count);
  return count;
#else
  //TODO； we need to copy the buffer value from the orginal buffer to a new created buffer here.
  // or we need to make every write synchronous because the orginal buffer will be freed after the function returns.
  // we need to add gallocator->WaitPendingRequest() after the write operation.
  //for asynchronous request, we must ensure the WorkRequest is valid after this function returns
  WorkRequest wr { };
  wr.op = WRITE;
  wr.flag = flag | ASYNC;
  wr.size = count;
  wr.addr = GADD(addr, offset);
  wr.ptr = buf;
#ifdef GFUNC_SUPPORT
  if (func) {
    wr.gfunc = func;
    wr.arg = arg;
    wr.flag |= GFUNC;
  }
#endif

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "write failed");
    return 0;
  } else {
    return wr.size;
  }
#endif
}

void GAlloc::MFence() {
#ifndef LOCAL_MEMORY_HOOK
  WorkRequest wr { };
  wr.op = MFENCE;
  wr.flag = ASYNC;
  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "MFence failed");
  }
#endif
}

void GAlloc::SFence() {
#ifndef LOCAL_MEMORY_HOOK
  WorkRequest wr { };
  wr.op = SFENCE;
  wr.flag = ASYNC;
  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "SFence failed");
  }
#endif
}

int GAlloc::Lock(Work op, const GAddr addr, const Size count, Flag flag) {
#ifdef LOCAL_MEMORY_HOOK
  return 0;
#else
  WorkRequest wr { };
  wr.op = op;
  wr.addr = addr;
#ifdef ASYNC_UNLOCK
  if (op == UNLOCK)
    flag |= ASYNC;
#endif
  wr.flag = flag;
  int i = 0, j = 0;
  GAddr start_blk = TOBLOCK(addr);
  GAddr end = GADD(addr, count - 1);
  GAddr end_blk = TOBLOCK(end);
  while (!wh->SendRequest(&wr)) {
    i++;
    GAddr next = GADD(start_blk, i*BLOCK_SIZE);
    if (next > end_blk)
      break;

    epicLog(LOG_DEBUG, "lock split to multiple blocks");
    wr.Reset();
    wr.op = op;
    wr.addr = next;
    wr.flag = flag;
    epicAssert(wr.addr % BLOCK_SIZE == 0);
  }
  if (op == UNLOCK) {
    epicAssert(!wr.status);
  } else {
    if (wr.status) {  //failed at ith lock
      if (i >= 1) {  //first lock succeed
        wr.Reset();
        wr.op = UNLOCK;
        wr.addr = addr;
        wr.flag = flag;
#ifdef ASYNC_UNLOCK
        wr.flag |= ASYNC;
#endif
        int ret = wh->SendRequest(&wr);
        epicAssert(!ret);
      }
      for (j = 1; j < i; j++) {
        wr.Reset();
        wr.addr = GADD(start_blk, j*BLOCK_SIZE);
        epicAssert(wr.addr % BLOCK_SIZE == 0);
        epicAssert(wr.addr <= end_blk);
        wr.op = UNLOCK;
        wr.flag = flag;
#ifdef ASYNC_UNLOCK
        wr.flag |= ASYNC;
#endif
        int ret = wh->SendRequest(&wr);
        epicAssert(!ret);
      }
      epicLog(LOG_DEBUG, "lock failed");
      return -1;
    }
  }
  epicLog(LOG_DEBUG, "lock succeed");
  return 0;
#endif
}

void GAlloc::RLock(const GAddr addr, const Size count) {
  Lock(RLOCK, addr, count);
}

void GAlloc::WLock(const GAddr addr, const Size count) {
  Lock(WLOCK, addr, count);
}

int GAlloc::Try_RLock(const GAddr addr, const Size count) {
  return Lock(RLOCK, addr, count, TRY_LOCK);
}

int GAlloc::Try_WLock(const GAddr addr, const Size count) {
  return Lock(WLOCK, addr, count, TRY_LOCK);
}

void GAlloc::UnLock(const GAddr addr, const Size count) {
  Lock(UNLOCK, addr, count);
}

Size GAlloc::Put(uint64_t key, const void* value, Size count) {
  WorkRequest wr { };
  wr.op = PUT;
  wr.size = count;
  wr.key = key;
  wr.ptr = const_cast<void*>(value);

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "Put failed");
    return 0;
  } else {
    return wr.size;
  }
}

Size GAlloc::Get(uint64_t key, void* value) {
  WorkRequest wr { };
  wr.op = GET;
  wr.key = key;
  wr.ptr = value;

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "Get failed");
    return 0;
  } else {
    return wr.size;
  }
}

#ifdef DHT
int GAlloc::HTable(void* addr) {
  WorkRequest wr{};
  wr.op = GET_HTABLE;
  wr.addr = (GAddr)addr;
  if (wh->SendRequest(&wr)) {
    return -1;
  } else {
    return 0;
  }
}
#endif

GAlloc::~GAlloc() {
  delete wh;
}

bool GAlloc::connectMemcached() {
    memcached_server_st *servers = NULL;
    memcached_return rc;

    std::ifstream conf("/users/Ruihong/gam/memcached_ip.conf");

    if (!conf) {
        fprintf(stderr, "can't open memcached_ip.conf\n");
        return false;
    }

    std::string addr, port;
    std::getline(conf, addr);
    std::getline(conf, port);

    memc = memcached_create(NULL);
    servers = memcached_server_list_append(servers, trim(addr).c_str(),
                                           std::stoi(trim(port)), &rc);
    rc = memcached_server_push(memc, servers);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Counld't add server:%s\n", memcached_strerror(memc, rc));
        sleep(1);
        return false;
    }

    memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
    return true;
}

bool GAlloc::disconnectMemcached() {
    if (memc) {
        memcached_quit(memc);
        memcached_free(memc);
        memc = NULL;
    }
    return true;
}
void GAlloc::memSet(const char *key, uint32_t klen, const char *val,
                  uint32_t vlen) {

    memcached_return rc;
    while (true) {
        memc_mutex.lock();

        rc = memcached_set(memc, key, klen, val, vlen, (time_t)0, (uint32_t)0);
        if (rc == MEMCACHED_SUCCESS) {
            memc_mutex.unlock();
            break;
        }else{
            memc_mutex.unlock();

        }

        usleep(400);
    }
}

char *GAlloc::memGet(const char *key, uint32_t klen, size_t *v_size) {

    size_t l;
    char *res;
    uint32_t flags;
    memcached_return rc;

    while (true) {
        memc_mutex.lock();
        res = memcached_get(memc, key, klen, &l, &flags, &rc);
        if (rc == MEMCACHED_SUCCESS) {
            memc_mutex.unlock();
            break;
        }else{
            memc_mutex.unlock();

        }
        usleep(200 * wh->GetWorkerId());
    }

    if (v_size != nullptr) {
        *v_size = l;
    }

    return res;
}

