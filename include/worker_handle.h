// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_WORKER_HANDLE_H_
#define INCLUDE_WORKER_HANDLE_H_

#include <mutex>
#include <atomic>

#include "lockwrapper.h"
#include "worker.h"
#include "workrequest.h"
struct cacheline_holder {
  char pad[64];
} __attribute__((aligned(64)));

class WorkerHandle {
  boost::lockfree::queue<WorkRequest*>* wqueue;  //work queue used to communicate with worker
  Worker* worker;
  //app-side pipe fd
  int send_pipe[2];
  int recv_pipe[2];
  std::atomic<int> registered_thread_num;
    static thread_local int thread_id;
    static LockWrapper lock;
#ifdef USE_PTHREAD_COND
  pthread_mutex_t cond_lock;
  pthread_cond_t cond;
#endif
#if !(defined(USE_PIPE_W_TO_H) && defined(USE_PIPE_H_TO_W))
  volatile cacheline_holder* notify_buf;// can support at most 32 concurrent threads.
  int notify_buf_size;
#endif
  public:
  WorkerHandle(Worker* w);
  void RegisterThread();
  void DeRegisterThread();
  int SendRequest(WorkRequest* wr);
  inline int GetWorkerId() {
    return worker->GetWorkerId();
  }
  int GetWorkersSize() {
    return worker->GetWorkersSize();
  }
  inline void* GetLocal(GAddr addr) {
    return worker->ToLocal(addr);
  }
  Size GetWriteReplyCounter() {
    return worker->GetWriteReplyCounter();
  }
    Size GetWriteHitCounter() {
        return worker->GetWriteHitCounter();
    }
  void ResetWriteReplyCounter() {
    worker->ResetWriteReplyCounter();
  }
  void ReportCacheStatistics();
  void ResetCacheStatistics();

  ~WorkerHandle();
};

#endif /* INCLUDE_WORKER_HANDLE_H_ */
