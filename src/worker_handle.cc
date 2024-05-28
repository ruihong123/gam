// Copyright (c) 2018 The GAM Authors 


#include <cstring>
#include "worker_handle.h"

#include "../include/lockwrapper.h"
#include "zmalloc.h"
#include "util.h"
#include "kernel.h"
bool buffer_is_not_all_zero(char* buf, int size);
LockWrapper WorkerHandle::lock;
thread_local int WorkerHandle::thread_id = -1;
WorkerHandle::WorkerHandle(Worker* w)
    : worker(w),
      wqueue(w->GetWorkQ()),
      registered_thread_num(0){
//#if !defined(USE_PIPE_W_TO_H) || (!defined(USE_BOOST_QUEUE) && !defined(USE_PIPE_H_TO_W))
#if !(defined(USE_PIPE_W_TO_H) && defined(USE_PIPE_H_TO_W))
  int notify_buf_size = sizeof(WorkRequest) + sizeof(int);
  int ret = posix_memalign((void**) &notify_buf, HARDWARE_CACHE_LINE,
                           HARDWARE_CACHE_LINE*32);
  epicAssert((uint64_t)notify_buf % HARDWARE_CACHE_LINE == 0 && !ret);
    for (int i = 0; i < 32; ++i) {
        *(int*)(&notify_buf[i]) = 2;
    }
#endif
#ifdef USE_PTHREAD_COND
  pthread_mutex_init(&cond_lock, NULL);
  pthread_cond_init(&cond, NULL);
  pthread_mutex_lock(&cond_lock);
#endif
  RegisterThread();
}

WorkerHandle::~WorkerHandle() {
  DeRegisterThread();
#if !defined(USE_PIPE_W_TO_H) || (!defined(USE_BOOST_QUEUE) && !defined(USE_PIPE_H_TO_W))
  free((void*) notify_buf);
#endif
#ifdef USE_PTHREAD_COND
  pthread_mutex_unlock(&cond_lock);
#endif
}

void WorkerHandle::RegisterThread() {
  if (pipe(send_pipe)) {
    epicLog(LOG_WARNING, "create send pipe failed");
  }
  if (pipe(recv_pipe)) {
    epicLog(LOG_WARNING, "create recv pipe failed");
  }
  worker->RegisterFence(recv_pipe[1]);
#if defined(USE_PIPE_W_TO_H) || defined(USE_PIPE_H_TO_W)
  worker->RegisterHandle(send_pipe[0]);
#endif
#ifndef USE_BOOST_QUEUE
  worker->RegisterNotifyBuf(notify_buf);
#endif
}

void WorkerHandle::DeRegisterThread() {
#if defined(USE_PIPE_W_TO_H) || defined(USE_PIPE_H_TO_W)
  worker->DeRegisterHandle(send_pipe[0]);
#endif
#ifndef USE_BOOST_QUEUE
  worker->DeRegisterNotifyBuf(notify_buf);
#endif
  if (close(send_pipe[0])) {
    epicLog(LOG_WARNING, "close send_pipe[0] (%d) failed: %s (%d)",
            send_pipe[0], strerror(errno), errno);
  }
  if (close(send_pipe[1])) {
    epicLog(LOG_WARNING, "close send_pipe[1] (%d) failed: %s (%d)",
            send_pipe[1], strerror(errno), errno);
  }
  if (close(recv_pipe[0])) {
    epicLog(LOG_WARNING, "close recv_pipe[0] (%d) failed: %s (%d)",
            recv_pipe[0], strerror(errno), errno);
  }
  if (close(recv_pipe[1])) {
    epicLog(LOG_WARNING, "close recv_pipe[1] (%d) failed: %s (%d)",
            recv_pipe[1], strerror(errno), errno);
  }
}

int WorkerHandle::SendRequest(WorkRequest* wr) {
    if (unlikely(thread_id == -1)) {
        thread_id = registered_thread_num.fetch_add(1);
    }
  wr->flag |= LOCAL_REQUEST;
#ifdef MULTITHREAD
  *(int*)(&notify_buf[thread_id]) = 1;  //not useful to set it to 1 if boost_queue is enabled
  wr->fd = recv_pipe[1];  //for legacy code
  //TOFIX(ruihong): the notify buf is not thread safe. Make the notify_buf thread safe.
  wr->notify_buf = (int*)&this->notify_buf[thread_id];
  epicLog(LOG_WARNING,
      "workid = %d, wr->notify_buf = %d, wr->op = %d, wr->flag = %d, wr->status = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d",
      worker->GetWorkerId(), *wr->notify_buf, wr->op, wr->flag, wr->status,
      wr->addr, wr->size, wr->fd);
    //In case that the pending to evict cache is too much causing cache memory explosion.
    //May be the code below will also block those operations which will not result in cache eviction. As a result, the design
    // is not good.
    while (worker->GetCacheToevict() > 512){
        usleep(5);
        if (worker->GetCacheToevict() > 16384){
            usleep(10);
            epicLog(LOG_INFO, "Cache to evict is too much %d", worker->GetCacheToevict());
        }

    }
    volatile int* local_notify_buf = wr->notify_buf;
    assert(*local_notify_buf == 1);
  int ret = worker->ProcessLocalRequest(wr);  //not complete due to remote or previously-sent similar requests
  if (ret) {  //not complete due to remote or previously-sent similar requests
    if (wr->flag & ASYNC) {
      return SUCCESS;
    } else {
#ifdef USE_PIPE_W_TO_H
      char buf[1];
      if(1 != read(recv_pipe[0], buf, 1)) {  //blocking
        epicLog(LOG_WARNING, "read notification from worker failed");
      } else {
        epicLog(LOG_DEBUG, "request returned %c", buf[0]);
        if(wr->status) {
          epicLog(LOG_INFO, "request failed %d\n", wr->status);
        }
      }
#elif defined(USE_PTHREAD_COND)
      int ret = pthread_cond_wait(&cond, &cond_lock);
      epicAssert(!ret);
#else
#ifdef TIME_PRINT
        auto statistic_start = std::chrono::high_resolution_clock::now();
#endif
        while (*local_notify_buf != 2){
            usleep(1);
        };
#ifndef NDEBUG
        if (wr->op == READ && wr->size > 8){
            assert(buffer_is_not_all_zero((char*)wr->ptr, wr->size));
            epicLog(LOG_WARNING, "get notified via buf %p size is %d", wr->ptr, wr->size);
        }
#endif
#ifdef TIME_PRINT
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - statistic_start);
        epicLog(LOG_WARNING,"Reply message polling  duration is %ld ns\n", duration.count());
#endif

#endif
      return wr->status;
    }
  } else {
    return wr->status;
  }
#else //if not MULTITHREAD

  WorkRequest* to = wr;
  char buf[1];
  buf[0] = 's';

  if(wr->flag & ASYNC) {  //if the work request is asynchronous, we return immediately
    //copy the workrequest
    to = wr->Copy();
  }

#ifdef USE_PIPE_W_TO_H
  to->fd = recv_pipe[1];
  wqueue->push(to);
#elif defined(USE_PTHREAD_COND)
  to->cond_lock = &cond_lock;
  to->cond = &cond;
  to->fd = recv_pipe[1];
  wqueue->push(to);
#else
#ifdef USE_BOOST_QUEUE
  *notify_buf = 1;  //not useful to set it to 1 if boost_queue is enabled
  epicAssert(*(int*)notify_buf == 1);
  to->fd = recv_pipe[1];//for legacy code
  to->notify_buf = this->notify_buf;
  wqueue->push(to);
#elif defined(USE_BUF_ONLY)
  *(WorkRequest**)(notify_buf+1) = to;
  to->fd = recv_pipe[1];  //for legacy code
  to->notify_buf = this->notify_buf;
  *notify_buf = 1;//put last since it will trigger the process
#else
  *notify_buf = 1;  //put first since notify_buf = 1 may happen after notify_buf = 2 by worker
  epicAssert(*(int*)notify_buf == 1);
  to->fd = recv_pipe[1];//for legacy code
  to->notify_buf = this->notify_buf;
  wqueue->push(to);
#endif
#endif

#ifndef USE_BUF_ONLY //otherwise, we have to return after the worker copy the data from notify_buf
  //we return directly without writing to the pipe to notify the worker thread
  if(to->flag & ASYNC) {
    epicLog(LOG_DEBUG, "asynchronous request");
    return SUCCESS;
  }
#endif

#ifdef USE_PIPE_H_TO_W
#ifdef WH_USE_LOCK
  if(lock.try_lock()) {
#endif
    if (1 != write(send_pipe[1], buf, 1)) {
      epicLog(LOG_WARNING, "write to pipe failed (%d:%s)", errno,
          strerror(errno));
    }

    //we write twice in order to reduce the epoll latency
    if (1 != write(send_pipe[1], buf, 1)) {
      epicLog(LOG_WARNING, "write to pipe failed (%d:%s)", errno,
          strerror(errno));
    }

#ifdef WH_USE_LOCK
    lock.unlock();
  }
#endif
#endif

#ifdef USE_PIPE_W_TO_H
  if(1 != read(recv_pipe[0], buf, 1)) {  //blocking
    epicLog(LOG_WARNING, "read notification from worker failed");
  } else {
    epicLog(LOG_DEBUG, "request returned %c", buf[0]);
    if(wr->status) {
      epicLog(LOG_INFO, "request failed %d\n", wr->status);
    }
  }
#elif defined(USE_PTHREAD_COND)
  int ret = pthread_cond_wait(&cond, &cond_lock);
  epicAssert(!ret);
#else
  while(*notify_buf != 2);
//while(atomic_read(notify_buf) != 2);
  epicLog(LOG_DEBUG, "get notified via buf");
#endif
  return wr->status;

#endif //MULTITHREAD end
}

void WorkerHandle::ReportCacheStatistics() {
    epicLog(LOG_WARNING, "Cache Statistics");
    fprintf(stdout, "lread lrhit lwrite lwhit rread rrhit rwrite rwhit\n");
    fprintf(stdout, "%lu\t%lu\t%lu\t%lu\t%lu\t%lu\t%lu\t%lu\t%lu\n"
                       , worker->no_local_reads_.load()
                       , worker->no_local_reads_hit_.load()
                       , worker->no_local_writes_.load()
                       , worker->no_local_writes_hit_.load()
                       , worker->no_remote_reads_.load()
                       , worker->no_remote_reads_hit_.load()
                       , worker->no_remote_writes_.load()
                       , worker->no_remote_writes_hit_.load()
                       , worker->no_remote_writes_direct_hit_.load());
}

void WorkerHandle::ResetCacheStatistics() {

    worker->no_local_reads_ = 0;
    worker->no_local_reads_hit_ = 0;

    worker->no_local_writes_ = 0;
    worker->no_local_writes_hit_ = 0;

    worker->no_remote_reads_ = 0;
    worker->no_remote_reads_hit_ = 0;

    worker->no_remote_writes_ = 0;
    worker->no_remote_writes_hit_ = 0;

    worker->no_remote_writes_direct_hit_ = 0;
}

void WorkerHandle::WaitPendingRequest() {
    worker->WaitPendingRequest();
}

