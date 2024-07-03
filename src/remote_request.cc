// Copyright (c) 2018 The GAM Authors 


#include <cstring>
#include <utility>
#include <queue>
#include "rdma.h"
#include "worker.h"
#include "anet.h"
#include "log.h"
#include "ae.h"
#include "client.h"
#include "util.h"
#include "structure.h"
#include "ae.h"
#include "tcp.h"
#include "slabs.h"
#include "zmalloc.h"
#include "kernel.h"
#include "chars.h"

#ifdef NOCACHE
#include "remote_request_nocache.cc"
#else //not NOCACHE
#include "remote_request_cache.cc"
#endif //NOCACHE

#ifdef DHT
void Worker::ProcessRemoteHTable(Client* cli, WorkRequest* wr) {
  epicLog(LOG_DEBUG, "node %d htable (%llu) recv'ed a remote htable request from %d", this->GetWorkerId(), htable, cli->GetWorkerId());
    wr->op = GET_HTABLE_REPLY;
    //epicAssert(htable != nullptr);
    if (htable != nullptr)
        wr->addr = this->ToGlobal(this->htable);
    else
        wr->addr = Gnullptr;
    wr->status = SUCCESS;
    this->SubmitRequest(cli, wr);
    delete wr;
}

void Worker::ProcessHTableReply(Client* cli, WorkRequest* wr) {
    static std::map<uint32_t, GAddr> htableMap;
    int cnt = 0;

    if (cli) {
        htableMap[cli->GetWorkerId()] = wr->addr;
    } else {
        htableMap[this->GetWorkerId()] = ToGlobal(this->htable);
    }

    if (htableMap.size() == this->widCliMapWorker.size()) {
        WorkRequest* pwr;
        if (cli)
            pwr = this->GetPendingWork(wr->id);
        else 
            pwr = wr;
        char* a = (char*)pwr->addr;
        for (auto& p : htableMap) {
            if (p.second != Gnullptr) {
                a += appendInteger(a, p.second);
                cnt++;
            }
        } 
        htableMap.clear();
        //pwr->ptr = (void*)addr;
        pwr->status = SUCCESS;
        if (cli)
            Notify(pwr);
    }

    if (cli)
        delete wr;
}
#endif // DHT

void Worker::ProcessRemoteMemStat(Client* client, WorkRequest* wr) {
  uint32_t qp;
  int wid;
  Size mtotal, mfree;
  vector<Size> stats;
  Split<Size>((char*) wr->ptr, stats);
  epicAssert(stats.size() == wr->size * 4);
  for (int i = 0; i < wr->size; i++) {
    qp = stats[i * 4];
    wid = stats[i * 4 + 1];
    mtotal = stats[i * 4 + 2];
    mfree = stats[i * 4 + 3];

    Client* cli = FindClientWid(wid);
    widCliMapWorker[wid] = cli;
    //		if(GetWorkerId() == wid) {
    //			epicLog(LOG_DEBUG, "Ignore self information");
    //			continue;
    //		}
    cli->lock();
    if (cli) {
      cli->SetMemStat(mtotal, mfree);
        epicLog(LOG_INFO, "worker %d 's memstats get updated, total mem is %llu, free mem is %llu", wid, mtotal, mfree);
    } else {
      epicLog(LOG_WARNING, "worker %d not registered yet", wid);
    }
    cli->unlock();
  }
  /*
   * we don't need wr any more
   */
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteMalloc(Client* client, WorkRequest* wr) {

  void* addr = nullptr;
  if (wr->flag & ALIGNED) {
    addr = sb.sb_aligned_malloc(wr->size);
    epicAssert((uint64_t)addr % BLOCK_SIZE == 0);
  } else {
    addr = sb.sb_malloc(wr->size);
  }
  //FIXME: remove below
  memset(addr, 0, wr->size);
  if (addr) {
    wr->addr = TO_GLOB(addr, base, GetWorkerId());
    wr->status = SUCCESS;
    ghost_size += wr->size;
    if (ghost_size > conf->ghost_th)
      SyncMaster();
    epicLog(LOG_DEBUG,
        "allocated %d at address %lx, base = %lx, wid = %d, gaddr = %lx",
        wr->size, addr, base, GetWorkerId(), wr->addr);
  } else {
    wr->status = ALLOC_ERROR;
  }
  wr->op = MALLOC_REPLY;
  SubmitRequest(client, wr);
  delete wr;
  wr = nullptr;

}

void Worker::ProcessRemoteMallocReply(Client* client, WorkRequest* wr) {
  WorkRequest* pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  if (wr->status) {
    epicLog(LOG_WARNING, "remote malloc error");
  }
  pwr->addr = wr->addr;
  pwr->status = wr->status;
  //	pending_works.erase(wr->id);
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  if (Notify(pwr)) {
    epicLog(LOG_FATAL, "cannot wake up the app thread");
  }
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteGetReply(Client* client, WorkRequest* wr) {
  WorkRequest* pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  pwr->status = wr->status;
  pwr->size = wr->size;

  if (wr->status) {
    epicAssert(!pwr->size);
    epicLog(LOG_WARNING, "cannot get the value for key %ld", wr->key);
  } else {
    memcpy(pwr->ptr, wr->ptr, wr->size);
  }

  if (Notify(pwr)) {
    epicLog(LOG_FATAL, "cannot wake up the app thread");
  }
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  delete wr;
  wr = nullptr;
}
// Remote cache evict a shared page.
void Worker::ProcessRemoteEvictShared(Client* client, WorkRequest* wr) {
  void* laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  DirEntry* entry = directory.GetEntry(laddr);
  if (directory.InTransitionState(entry)) {
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in transition state %d",
        directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }
  directory.Clear(entry, client->ToGlobal(wr->ptr));
  directory.unlock(laddr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteEvictDirty(Client* client, WorkRequest* wr) {
  void* laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  DirEntry* entry = directory.GetEntry(laddr);
  if (directory.InTransitionState(entry)) {
    //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in transition state %d",
        directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }
  directory.Clear(entry, client->ToGlobal(wr->ptr));
  directory.unlock(laddr);
    epicLog(LOG_INFO, "cache write back\n",
            directory.GetState(entry));
  client->WriteWithImm(nullptr, nullptr, 0, wr->id);
  epicLog(LOG_INFO, "Cache entry eviction message for %llu is processed on the home node", wr->addr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRequest(Client* client, WorkRequest* wr) {
  epicLog(LOG_DEBUG, "process remote request %d from worker %d", wr->op,
      client->GetWorkerId());
  epicAssert(wr->wid == 0 || wr->wid == client->GetWorkerId());

  switch (wr->op) {

#ifdef DHT
    case GET_HTABLE: 
    {
        this->ProcessRemoteHTable(client, wr);
        break;
    }
    case GET_HTABLE_REPLY:
    {
        this->ProcessHTableReply(client, wr);
        break;
    }
#endif //DHT

    case FETCH_MEM_STATS_REPLY:
    case BROADCAST_MEM_STATS: 
      {
        ProcessRemoteMemStat(client, wr);
        break;
      }
    case MALLOC: 
      {
        ProcessRemoteMalloc(client, wr);
        break;
      }
    case MALLOC_REPLY: 
      {
        ProcessRemoteMallocReply(client, wr);
        break;
      }
    case FREE: 
      {
        //FIXME: check whether other nodes are sharing this data
        //issue a write request first, and then process the free
        epicAssert(IsLocal(wr->addr));
        Size size = sb.sb_free(ToLocal(wr->addr));
        ghost_size -= size;
        if (ghost_size.load() > conf->ghost_th)
          SyncMaster();
        delete wr;
        wr = nullptr;
        break;
      }
    case GET_REPLY: 
      {
        ProcessRemoteGetReply(client, wr);
        break;
      }
    case READ:
      {
        ProcessRemoteRead(client, wr);
        break;
      }
    case READ_FORWARD:
    case FETCH_AND_SHARED: 
      {
        ProcessRemoteReadCache(client, wr);
        break;
      }
    case READ_REPLY:
      {
        ProcessRemoteReadReply(client, wr);
        break;
      }
    case WRITE:
    case WRITE_PERMISSION_ONLY: 
      {
        ProcessRemoteWrite(client, wr);
        break;
      }
    case INVALIDATE:
    case FETCH_AND_INVALIDATE:
    case WRITE_FORWARD:
    case INVALIDATE_FORWARD:
    case WRITE_PERMISSION_ONLY_FORWARD: 
      {
        ProcessRemoteWriteCache(client, wr);
        break;
      }
    case WRITE_REPLY:
      {
        ProcessRemoteWriteReply(client, wr);
        break;
      }
    case ACTIVE_INVALIDATE:
      {
        ProcessRemoteEvictShared(client, wr);
        break;
      }
    case WRITE_BACK:
      {
        ProcessRemoteEvictDirty(client, wr);
        break;
      }
#ifdef NOCACHE
    case RLOCK:
      ProcessRemoteRLock(client, wr);
      break;
    case WLOCK:
      ProcessRemoteWLock(client, wr);
      break;
    case RLOCK_REPLY:
    case WLOCK_REPLY:
      ProcessRemoteLockReply(client, wr);
      break;
    case UNLOCK:
      ProcessRemoteUnLock(client, wr);
      break;
#ifndef ASYNC_UNLOCK
    case UNLOCK_REPLY:
      ProcessRemoteUnLockReply(client, wr);
      break;
#endif
#endif
    default:
      epicLog(LOG_WARNING, "unrecognized request from %d",
          client->GetWorkerId());
      exit(-1);
      break;
  }
}
//Remote means request issued from the remote side.
void Worker::ProcessRemoteRead(Client* client, WorkRequest* wr) {
    epicAssert(IsLocal(wr->addr));
    epicLog(LOG_INFO, "Process remote READ request from %d", client->GetWorkerId());
#ifdef SELECTIVE_CACHING
    void* laddr = ToLocal(TOBLOCK(wr->addr));
#else
    void* laddr = ToLocal(wr->addr);
#endif
    directory.lock(laddr);
    DirEntry* entry = directory.GetEntry(laddr);
    if (directory.InTransitionState(entry)) {
        //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
        AddToServeRemoteRequest(wr->addr, client, wr);
        epicLog(LOG_INFO, "directory in Transition State %d",
                directory.GetState(entry));
        directory.unlock(laddr);
        return;
    }
    if (directory.GetState(entry) != DIR_DIRTY) {  //it is shared or exclusively owned (Case 2)
        //add the lock support
        if (directory.IsBlockWLocked(entry)) {
            //According to my analysis, the code below is never invoked.
            assert(false);
            if (wr->flag & TRY_LOCK) {  //reply directly with lock failed
                epicAssert(wr->flag & LOCKED);
                wr->status = LOCK_FAILED;
                wr->op = READ_REPLY;
                directory.unlock(laddr);
                SubmitRequest(client, wr);
                delete wr;
                wr = nullptr;
            } else {
                //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
                AddToServeRemoteRequest(wr->addr, client, wr);
                directory.unlock(laddr);
            }
            epicLog(LOG_INFO, "addr %lx is exclusively locked by %d", ToGlobal(laddr),
                    GetWorkerId());
            return;
        }

        //TODO: add the write completion check
        epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
        client->WriteWithImm(wr->ptr, ToLocal(wr->addr), wr->size, wr->id);
        epicLog(LOG_INFO, "Remote read write back to node %d, RDMA write with imm, local addr %p, remote addr %p", client->GetWorkerId(), ToLocal(wr->addr),
                wr->ptr);
#ifdef SELECTIVE_CACHING
        if(!(wr->flag & NOT_CACHE)) {
#endif
        if (entry) {
            epicAssert(
                    directory.GetState(entry) == DIR_UNSHARED
                    || directory.GetState(entry) == DIR_SHARED);
            directory.ToShared(entry, client->ToGlobal(wr->ptr));
        } else {
            epicAssert(directory.GetState(entry) == DIR_UNSHARED);
            directory.ToShared(laddr, client->ToGlobal(wr->ptr));
        }
#ifdef SELECTIVE_CACHING
        }
#endif
        delete wr;
        wr = nullptr;
    } else {
        epicAssert(!directory.IsBlockLocked(entry));
        WorkRequest* lwr = new WorkRequest(*wr);
        lwr->counter = 0;
        lwr->op = READ_FORWARD;
        lwr->parent = wr;
        lwr->pid = wr->id;
        lwr->pwid = client->GetWorkerId();

        GAddr rc = directory.GetSList(entry).front();  //only one worker is updating this line
        Client* cli = GetClient(rc);
#ifdef SELECTIVE_CACHING
        if(!(wr->flag & NOT_CACHE)) {
      //intermediate state
      directory.ToToShared(entry);
      SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
    } else {
      SubmitRequest(cli, lwr);
    }
#else
        //intermediate state
        directory.ToToShared(entry);
        epicLog(LOG_INFO, "Remote read forward, conducted thorugh RDMA send");
                SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
#endif
    }
    directory.unlock(laddr);
}
// Handle the read forward request, this node has to exclusively own the data.
void Worker::ProcessRemoteReadCache(Client* client, WorkRequest* wr) {
    Work op_orin = wr->op;
    bool deadlock = false;
#ifndef SELECTIVE_CACHING
    epicAssert(BLOCK_ALIGNED(wr->addr));
#endif
    GAddr blk = TOBLOCK(wr->addr);
    cache.lock(blk);
    CacheLine* cline = cache.GetCLine(blk);
    if (!cline) {
        epicLog(LOG_FATAL, "Unexpected: cannot find an updated copy");
        wr->op = READ_REPLY;  //change op to the corresponding reply type
        wr->status = READ_ERROR;
        if (FETCH_AND_SHARED == op_orin) {
            SubmitRequest(client, wr);
        } else {  //READ_FORWARD
            SubmitRequest(client, wr);  //reply to the home node
            Client* cli = FindClientWid(wr->pwid);
            wr->id = wr->pid;
            SubmitRequest(cli, wr);  //reply to the local node
        }
    } else {
        if (cache.InTransitionState(cline->state)) {
            if (cline->state == CACHE_TO_DIRTY) {
                //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
                AddToServeRemoteRequest(wr->addr, client, wr);
                epicLog(LOG_INFO, "cache in transition state %d", cline->state);
                cache.unlock(blk);
                return;
            } else {
                //deadlock: this node wants to give up the ownership
                //meanwhile, another node wants to read
                epicLog(LOG_INFO, "!!!deadlock detected!!!\n");
                epicAssert(cline->state == CACHE_TO_INVALID);
                deadlock = true;
            }
        }

        //add the lock support
        if (cache.IsBlockWLocked(cline)) {
            if (wr->flag & TRY_LOCK) {  //reply directly with lock failed
                epicAssert(wr->flag & LOCKED);
                wr->status = LOCK_FAILED;
                wr->op = READ_REPLY;
                if (FETCH_AND_SHARED == op_orin) {
                    SubmitRequest(client, wr);
                } else {  //READ_FORWARD
                    SubmitRequest(client, wr);  //reply to the home node
                    Client* cli = FindClientWid(wr->pwid);
                    wr->id = wr->pid;
                    SubmitRequest(cli, wr);  //reply to the local node
                }
                delete wr;
                wr = nullptr;
                cache.unlock(blk);
            } else {
                epicAssert(!deadlock);
                //we must unlock the cache/directory lock before calling the AddToServe[Remote]Request
                //as the lock acquire seq is fences -> directory/cache -> to_serve_local/remote_request/pending_works
                //the ProcessToServeRequest() breaks this rule
                //we copy the queue first and then release the to_serve.._request lock immediately
                //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
                AddToServeRemoteRequest(wr->addr, client, wr);
                cache.unlock(blk);
            }
            epicLog(LOG_INFO, "addr %lx is exclusively locked by %d", blk,
                    GetWorkerId());
            return;
        }

        //TODO: add the write completion check
        //can add it to the pending work and check it upon done
        if (op_orin == FETCH_AND_SHARED) {
#ifdef SELECTIVE_CACHING
            epicAssert(wr->size == BLOCK_SIZE && wr->addr == blk);
#endif
            epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
            client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);  //reply to the local home node
        } else {  //READ_FORWARD
            Client* cli = FindClientWid(wr->pwid);

#ifdef SELECTIVE_CACHING
            void* cs = (void*)((ptr_t)cline->line + GMINUS(wr->addr, blk));
      if(!(wr->flag & NOT_CACHE)) {
        epicAssert(wr->size == BLOCK_SIZE && wr->addr == blk && cs == cline->line);
      }
      epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
      cli->WriteWithImm(wr->ptr, cs, wr->size, wr->pid);  //reply to the local node
      if(!(wr->flag & NOT_CACHE)) {
        epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
        client->WriteWithImm(client->ToLocal(blk), cline->line, BLOCK_SIZE, wr->id);  //writeback to home node
      }
#else
            epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
            cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid);  //reply to the local node
            client->WriteWithImm(client->ToLocal(blk), cline->line, BLOCK_SIZE,
                                 wr->id);  //writeback to home node
#endif
        }

#ifdef SELECTIVE_CACHING
        if(!(wr->flag & NOT_CACHE)) {
#endif
        //TOOD: add below to the callback function
        if (!deadlock)
            cache.ToShared(cline);
#ifdef SELECTIVE_CACHING
        }
#endif
    }
    cache.unlock(blk);
    delete wr;
    wr = nullptr;
}

void Worker::ProcessRemoteReadReply(Client* client, WorkRequest* wr) {
    /*
     * READ/RLock failed case,
     * as normal successful case is through write_with_imm
     * which will call ProcessRequest(Client*, unsigned int)
     */
    epicAssert(READ_ERROR == wr->status || LOCK_FAILED == wr->status);
    WorkRequest* pwr = GetPendingWork(wr->id);
    epicAssert(pwr);
    epicAssert(pwr->id == wr->id);
    epicAssert((pwr->flag & LOCKED) && (pwr->flag & TRY_LOCK));
    switch (pwr->op) {
        case READ:  //local node
        case FETCH_AND_SHARED:  //local and home node
        {
            //For read, it's ok to undo the change and clear pending work now
            //as there should be only one responder
            epicAssert(pwr->parent);
            pwr->parent->lock();

            //undo the directory/cache changes
            if (READ == pwr->op) {
                epicAssert(pwr->flag & CACHED);
                cache.lock(pwr->addr);
                cache.ToInvalid(pwr->addr);
                cache.unlock(pwr->addr);
            } else {  //FETCH_AND_SHARED
                epicAssert(IsLocal(pwr->addr));
                epicAssert(pwr->ptr == ToLocal(pwr->addr));
                directory.lock(pwr->ptr);
                directory.UndoDirty(pwr->ptr);
                directory.unlock(pwr->ptr);
            }

            pwr->parent->status = wr->status;
            int ret = ErasePendingWork(wr->id);
            epicAssert(ret);
            pwr->parent->unlock();
            Notify(pwr->parent);
            break;
        }
        case READ_FORWARD:  //home node
        {
            epicAssert(pwr->parent);
            epicAssert(IsLocal(pwr->addr));  //I'm the home node
            //parent request is from local node
            WorkRequest* parent = pwr->parent;
            void* laddr = ToLocal(pwr->addr);

            //For read, it's ok to undo the change and clear pending work now
            //as there should be only one responder
            directory.lock(laddr);
            directory.UndoDirty(laddr);
            directory.unlock(laddr);
            int ret = ErasePendingWork(wr->id);
            epicAssert(ret);
            delete parent;
            parent = nullptr;
            break;
        }
        default:
            epicLog(LOG_WARNING,
                    "Unrecognized pending work request %d for WRITE_REPLY", pwr->op);
            exit(-1);
            break;
    }

    ProcessToServeRequest(pwr);
    delete pwr;
    delete wr;
    pwr = nullptr;
    wr = nullptr;
}
// process write request from the remote side. happen in the event loop
void Worker::ProcessRemoteWrite(Client* client, WorkRequest* wr) {
    Work op_orin = wr->op;
#ifndef SELECTIVE_CACHING
    epicAssert(wr->size == BLOCK_SIZE);
    epicAssert(BLOCK_ALIGNED(wr->addr));
#endif
    epicAssert(IsLocal(wr->addr));  //I'm the home node
#ifdef SELECTIVE_CACHING
    void* laddr = ToLocal(TOBLOCK(wr->addr));
#else
    void* laddr = ToLocal(wr->addr);
#endif
    epicAssert(BLOCK_ALIGNED((uint64_t)laddr));
    directory.lock(laddr);
    DirEntry* entry = directory.GetEntry(laddr);
    DirState state = directory.GetState(entry);
    if (directory.InTransitionState(state)) {
        AddToServeRemoteRequest(wr->addr, client, wr);
        epicLog(LOG_INFO, "Directory in Transition State %d", state);
        directory.unlock(laddr);
        return;
    }
    if (state != DIR_DIRTY) {
        //add the lock support
        if (directory.IsBlockLocked(entry)) {
            epicAssert(false);
            epicAssert((directory.IsBlockWLocked(entry) && state == DIR_UNSHARED)
                       || !directory.IsBlockWLocked(entry));
            if (wr->flag & TRY_LOCK) {  //reply directly with lock failed
                epicAssert(wr->flag & LOCKED);
                wr->status = LOCK_FAILED;
                wr->op = WRITE_REPLY;
                wr->counter = 0;
                SubmitRequest(client, wr);
                delete wr;
                wr = nullptr;
                directory.unlock(laddr);
            } else {
                AddToServeRemoteRequest(wr->addr, client, wr);
                directory.unlock(laddr);
            }
            epicLog(LOG_INFO, "addr %lx is locked by %d", ToGlobal(laddr),
                    GetWorkerId());
            return;
        }

        if (state == DIR_SHARED) {
            epicLog(LOG_INFO, "DIR_SHARED Process remote write request from %d, wr id is %u", client->GetWorkerId(),  wr->id);
            //change the invalidate strategy (home node accepts invalidation responses)
            //in order to simply the try_lock failed case
            list<GAddr>& shared = directory.GetSList(entry);
            WorkRequest* lwr = new WorkRequest(*wr);
#ifdef SELECTIVE_CACHING
            if(wr->flag & NOT_CACHE) {
        epicAssert(wr->size <= BLOCK_SIZE);
        lwr->addr = TOBLOCK(wr->addr);
        lwr->size = BLOCK_SIZE;
        lwr->ptr = (void*)((ptr_t)lwr->ptr - GMINUS(wr->addr, lwr->addr)); //not necessary
      }
#endif

            lwr->lock();
            lwr->counter = 0;
            lwr->op = INVALIDATE_FORWARD;
            lwr->parent = wr;
            lwr->id = GetWorkPsn();
            lwr->pwid = client->GetWorkerId();
            lwr->counter = shared.size();
            bool first = true;
            for (auto it = shared.begin(); it != shared.end(); it++) {
                Client* cli = GetClient(*it);
                if (cli == client) {
                    epicAssert(op_orin == WRITE_PERMISSION_ONLY);
                    lwr->counter--;
                    continue;
                }
                epicLog(LOG_INFO, "invalidate forward (%d) cache from worker %d",
                        lwr->op, cli->GetWorkerId());
                if (first) {
                    AddToPending(lwr->id, lwr);
                    first = false;
                }
                SubmitRequest(cli, lwr);
                //lwr->counter++;
            }

            if (lwr->counter) {
                lwr->unlock();
                directory.ToToDirty(entry);
                directory.unlock(laddr);
                return;  //return and wait for reply
            } else {
                lwr->unlock();
                epicAssert(op_orin == WRITE_PERMISSION_ONLY);
                delete lwr;
                lwr = nullptr;
            }
        } else {  //DIR_UNSHARED
#ifdef SELECTIVE_CACHING
            if(wr->flag & NOT_CACHE) {
#ifdef GFUNC_SUPPORT
        if(wr->flag & GFUNC) {
          epicAssert(wr->gfunc);
          epicAssert(TOBLOCK(wr->addr) == TOBLOCK(GADD(wr->addr, wr->size-1)));
          void* laddr = ToLocal(wr->addr);
          wr->gfunc(laddr, wr->arg);
        } else {
#endif
          memcpy(ToLocal(wr->addr), wr->ptr, wr->size);
#ifdef GFUNC_SUPPORT
        }
#endif
      } else {
#endif
            if (WRITE == op_orin) {
                epicLog(LOG_INFO, "write the local data %p  ro node %d (size = %ld) to destination %p",
                        laddr, client->GetWorkerId(),wr->size, wr->ptr);
                epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
                // In this case, the compute node does  not have a copy of data, we need to first write the local data
                // back to the request node.
                client->Write(wr->ptr, laddr, wr->size);
            } else {  //WRITE_PERMISSION_ONLY
                epicAssert(state == DIR_UNSHARED);
                //deadlock: one node (Node A) wants to update its cache from shared to dirty,
                //but at the same time, the home nodes invalidates all its shared copy (due to a local write)
                //currently, dir_state == dir_unshared (after pend the request because it was dir_to_unshared)
                //solution: Node A acts as it is still a shared copy so that the invalidation can completes,
                //after which, home node processes the pending list and change the WRITE_PERMISSION_ONLY to WRITE
                epicLog(LOG_DEBUG, "write the data to destination");
                epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
                client->Write(wr->ptr, laddr, wr->size);
                epicLog(LOG_INFO, "deadlock detected");
            }
#ifdef SELECTIVE_CACHING
            }
#endif
        }
        epicAssert(!directory.InTransitionState(entry));
        wr->op = WRITE_REPLY;
        wr->status = SUCCESS;
        wr->counter = 0;
        //Why not use write with imm? notify the remote node that the data is ready.
        epicLog(LOG_INFO, "Notify the request node for REMOTE write request on unshared data by RDMA send");
        SubmitRequest(client, wr);

#ifdef SELECTIVE_CACHING
        if(!(wr->flag & NOT_CACHE)) {
#endif

        //we can safely change the directory as we've already transfered the data to the local node
        // logging
        logOwner(client->GetWorkerId(), wr->addr);
        if (entry) {
            directory.ToDirty(entry, client->ToGlobal(wr->ptr));
        } else {
            directory.ToDirty(laddr, client->ToGlobal(wr->ptr));  //entry is null
        }

#ifdef SELECTIVE_CACHING
        }
#endif
        delete wr;
        wr = nullptr;
    } else {  //Case 4
        epicLog(LOG_INFO, "DIR_DIRTY Process remote write request from %d, wr id is %u", client->GetWorkerId(),  wr->id);

        epicAssert(!directory.IsBlockLocked(entry));
        WorkRequest* lwr = new WorkRequest(*wr);
#ifdef SELECTIVE_CACHING
        if (wr->flag & NOT_CACHE) {
      epicAssert(wr->size <= BLOCK_SIZE);
      lwr->addr = TOBLOCK(wr->addr);
      lwr->size = BLOCK_SIZE;
      lwr->ptr = (void*)((ptr_t)lwr->ptr - GMINUS(wr->addr, lwr->addr)); //not necessary
    }
#endif
        lwr->counter = 0;
        if (WRITE == op_orin || WLOCK == op_orin) {
            lwr->op = WRITE_FORWARD;
        } else if (WRITE_PERMISSION_ONLY == op_orin) {
            //deadlock: WRITE_PERMISSION_ONLY shouldn't co-exist with DIR_DIRTY state
            //there must be a race where one nodes (Node A) tries to update its cache from shared to dirty,
            //while another node (Node B) writes the data before that node
            //solution: Node A replies as its cache line is shared, and home node changes it to WRITE_FORWARD
            //lwr->op = WRITE_PERMISSION_ONLY_FORWARD;
            lwr->op = WRITE_FORWARD;
        }
        lwr->parent = wr;
        lwr->pid = wr->id;
        lwr->pwid = client->GetWorkerId();

        GAddr rc = directory.GetSList(entry).front();  //only one worker is updating this line
        Client* cli = GetClient(rc);

        //intermediate state
        directory.ToToDirty(entry);
        epicLog(LOG_INFO, "Write forward for dirty data page write, conducted through RDMA send");

        SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);// what does ADD_TO_PENDING mean?
    }
    directory.unlock(laddr);
}
// Remote write to the cache in this worker
void Worker::ProcessRemoteWriteCache(Client* client, WorkRequest* wr) {
    epicAssert(wr->op != WRITE_PERMISSION_ONLY_FORWARD);  //this cannot happen
    epicLog(LOG_INFO, "Write Cache send entrance to %d with pid %d", wr->pwid, wr->pid);

    Work op_orin = wr->op;
    bool deadlock = false;
    epicAssert(wr->size == BLOCK_SIZE);
    epicAssert(BLOCK_ALIGNED(wr->addr));
    epicAssert(!IsLocal(wr->addr));  //I'm not the home node
    //we hold an updated copy of the line (WRITE_FORWARD: Case 4)
    GAddr to_lock = wr->addr;
    cache.lock(to_lock);
    CacheLine* cline = cache.GetCLine(wr->addr);
    if (!cline) {
        epicLog(LOG_INFO, "Cache miss");

        if (INVALIDATE == op_orin || INVALIDATE_FORWARD == op_orin) {
            //this should because of cache line eviction from shared to invalid
            //so we reply as if it is shared
            deadlock = true;

            //TODO: add the write completion check
            //can add it to the pending work and check it upon done
            if (wr->op == INVALIDATE) {  //INVALIDATE
                client->WriteWithImm(nullptr, nullptr, 0, wr->id);
            } else {  //INVALIDATE_FORWARD
                //			Client* cli = FindClientWid(wr->pwid);
                //			cli->WriteWithImm(nullptr, nullptr, 0, wr->pid); //reply the new owner
                //			epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
                //      after change the invalidate_forward strategy
                client->WriteWithImm(nullptr, nullptr, 0, wr->id);
                epicLog(LOG_DEBUG, "send to %d with id %d", client->GetWorkerId(),
                        wr->id);
            }
        } else {
            epicLog(LOG_FATAL, "Unexpected: cannot find an updated copy");
            wr->op = WRITE_REPLY;  //change op to the corresponding reply type
            wr->status = WRITE_ERROR;
            if (INVALIDATE == op_orin || FETCH_AND_INVALIDATE == op_orin) {
                SubmitRequest(client, wr);
            } else if (INVALIDATE_FORWARD == op_orin) {
                //			Client* cli = FindClientWid(wr->pwid);
                //			wr->id = wr->pid;
                //			SubmitRequest(cli, wr);
                SubmitRequest(client, wr);
            } else {  //WRITE_FORWARD or WRITE_PERMISSION_ONLY_FORWARD
                SubmitRequest(client, wr);
                Client* cli = FindClientWid(wr->pwid);
                wr->id = wr->pid;
                SubmitRequest(cli, wr);
            }
        }
        delete wr;
        wr = nullptr;
    } else {
        if (cache.InTransitionState(cline->state)) {
            /*
             * deadlock, since the responding node must just change its cache state
             * and send request to home node,
             * who was not notified of the change and sent an invalidate/forward request.
             * How to solve?
             * there are two causes: cache from shared to dirty (ToDirty State)
             * cache from dirty to invalid (ToInvalid state)
             */

            if ((INVALIDATE == wr->op || INVALIDATE_FORWARD == wr->op)
                && cline->state == CACHE_TO_DIRTY) {
                //deadlock case 1
                epicLog(LOG_INFO, "!!!deadlock detected!!!");
                deadlock = true;
            } else {
                if (cline->state == CACHE_TO_INVALID) {
                    //deadlock case 2
                    epicLog(LOG_INFO, "!!!deadlock detected!!!");
                    deadlock = true;
                } else {
                    AddToServeRemoteRequest(wr->addr, client, wr);
                    epicLog(LOG_INFO, "cache in transition state %d", cline->state);
                    cache.unlock(to_lock);
                    return;
                }
            }
        }
//TODO: the following code can result in deadlock need to understand why.

//add the lock support
        if (cache.IsBlockLocked(cline)) {

            if (wr->flag & TRY_LOCK) {  //reply directly with lock failed
                epicAssert(wr->flag & LOCKED);
                wr->status = LOCK_FAILED;
                wr->op = WRITE_REPLY;
                if (INVALIDATE == op_orin || FETCH_AND_INVALIDATE == op_orin) {
                    SubmitRequest(client, wr);
                } else if (INVALIDATE_FORWARD == op_orin) {
                    //				Client* cli = FindClientWid(wr->pwid);
                    //				wr->id = wr->pid;
                    //				SubmitRequest(cli, wr);
                    SubmitRequest(client, wr);
                } else {  //WRITE_FORWARD or WRITE_PERMISSION_ONLY_FORWARD
                    SubmitRequest(client, wr);
                    Client* cli = FindClientWid(wr->pwid);
                    wr->id = wr->pid;
                    SubmitRequest(cli, wr);
                }
                cache.unlock(to_lock);
                delete wr;
                wr = nullptr;
                return;
            } else {
                //deadlock case 3
                //if it is rlocked, and in deadlock status (in transition state from shared to dirty)
                //we are still safe to act as it was in shared state and ack the invalidation request
                //because the intransition state will block other r/w requests
                //until we get replies from the home node (then WRITE_PERMISSION_ONLY has
                //been changed to WRITE by the home node as agreed)
                if (!deadlock) {
//                    if (wr->flag & LOCKED){
                        AddToServeRemoteRequest(wr->addr, client, wr);
                        epicLog(LOG_INFO, "addr %lx is locked by %d", wr->addr,
                                GetWorkerId());
                        cache.unlock(to_lock);
                        return;
//                    }

                } else {
                    epicLog(LOG_INFO, "Deadlock detected");
                }
            }
        }

        //TODO: add the write completion check
        //can add it to the pending work and check it upon done
        if (wr->op == FETCH_AND_INVALIDATE) {  //FETCH_AND_INVALIDATE
            assert(false); //Not gonna happen in disaggregated setup.
            epicAssert(cache.IsDirty(cline) || cache.InTransitionState(cline));
            if (deadlock) {
                epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
                client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);
                delete wr;
                wr = nullptr;
            } else {
                //			client->WriteWithImm(wr->ptr, line, wr->size, wr->id);
                //			cache.ToInvalid(wr->addr);
                //			delete wr;
                unsigned int orig_id = wr->id;
                wr->status = deadlock;
                wr->id = GetWorkPsn();
                wr->op = PENDING_INVALIDATE;
                AddToPending(wr->id, wr);
                cache.ToToInvalid(cline);
                epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
                client->WriteWithImm(wr->ptr, cline->line, wr->size, orig_id, wr->id,
                                     true);
            }

        } else if (wr->op == INVALIDATE) {  //INVALIDATE
            epicAssert(!cache.IsDirty(cline));
            client->WriteWithImm(nullptr, nullptr, 0, wr->id);
            //TOOD: add below to the callback function
            if (!deadlock)
                cache.ToInvalid(cline);
            delete wr;
            wr = nullptr;
        } else if (wr->op == INVALIDATE_FORWARD) {  //INVALIDATE_FORWARD
            epicAssert(!cache.IsDirty(cline));
            //		Client* cli = FindClientWid(wr->pwid);
            //		cli->WriteWithImm(nullptr, nullptr, 0, wr->pid); //reply the new owner
            //		epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
            client->WriteWithImm(nullptr, nullptr, 0, wr->id);
            epicLog(LOG_DEBUG, "send to %d with id %d", client->GetWorkerId(),
                    wr->id);
            if (!deadlock)
                cache.ToInvalid(cline);
            delete wr;
            wr = nullptr;
        } else {  //WRITE_FORWARD
            assert(wr->op == WRITE_FORWARD);
            Client* cli = FindClientWid(wr->pwid);//New owner.
            if (deadlock) {
#ifdef SELECTIVE_CACHING
                if(wr->flag & NOT_CACHE) {
          //client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);  //transfer ownership
          //fix bug here (wr->ptr is not the same as ToLocal(wr->addr)
          //and here we write the dirty data back to the home node rather than
          //the local node requesting the data
          epicAssert(BLOCK_ALIGNED(wr->addr));
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->WriteWithImm(client->ToLocal(wr->addr), cline->line, wr->size, wr->id);  //transfer ownership
        } else {
#endif
                epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
                    //this should be processed
                cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid);  //reply the new owner
                epicLog(LOG_INFO, "send to %d with pid %d", wr->pwid, wr->pid);
                client->WriteWithImm(nullptr, nullptr, 0, wr->id);  //transfer ownership
#ifdef SELECTIVE_CACHING
                }
#endif
                delete wr;
                wr = nullptr;
            } else {
                //		  cli->WriteWithImm(wr->ptr, line, wr->size, wr->pid); //reply the new owner
                //		  epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
                //			client->WriteWithImm(nullptr, nullptr, 0, wr->id); //transfer ownership
                //			cache.ToInvalid(wr->addr);
                //			delete wr;
                unsigned int orig_id = wr->id;
                epicLog(LOG_INFO, "send to %d with pid %d", wr->pwid, wr->pid);
                wr->id = GetWorkPsn();
                wr->op = PENDING_INVALIDATE;
                // todo: implement the pending mechanism like the way in cache eviction write back. use send to generate a write back.
                //  Or directly mark invalid here. no need to add to pending and mark as intermidiate state.
                AddToPending(wr->id, wr);
                cache.ToToInvalid(cline);
#ifdef SELECTIVE_CACHING
                if(wr->flag & NOT_CACHE) {
          epicAssert(BLOCK_ALIGNED(wr->addr));
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->WriteWithImm(client->ToLocal(wr->addr), cline->line, wr->size, orig_id, wr->id, true);  //transfer ownership
        } else {
#endif
                epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
                cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid, wr->id,
                                  true);  //reply the new owner
                client->WriteWithImm(nullptr, nullptr, 0, orig_id);  //transfer ownership
                //mark invalidate after forward and write back
//                cache.ToInvalid(cline);
#ifdef SELECTIVE_CACHING
                }
#endif
            }
        }
    }
    cache.unlock(to_lock);
}

void Worker::ProcessRemoteWriteReply(Client* client, WorkRequest* wr) {
//    write_reply_counter.fetch_add(1);
    WorkRequest* pwr = GetPendingWork(wr->id);
    epicAssert(pwr);
    epicAssert(pwr->id == wr->id);
    if (wr->status) {
        //don't need to lock parent
        //backup these data to make sure it is valid even delete pwr
        Flag flag = pwr->flag;
        GAddr addr = pwr->addr;

        WorkRequest* parent = pwr->parent;
        epicAssert(parent);
        parent->lock();

        if (IsLocal(addr)) {
            directory.lock(ToLocal(addr));
        } else {
            cache.lock(addr);
        }
        pwr->lock();

        epicAssert(LOCK_FAILED == wr->status);  //for now, only this should happen
        epicAssert((pwr->flag & LOCKED) && (pwr->flag & TRY_LOCK));
        epicLog(LOG_INFO, "wr->status = %d, op = %d", wr->status, pwr->op);
        epicLog(LOG_INFO, "write to %lx failed", pwr->addr);

        switch (pwr->op) {
            case WRITE:  //local node (invalid)
            case WRITE_PERMISSION_ONLY:  //local node (shared)
            case FETCH_AND_INVALIDATE:  //local and home node (dirty)
            case INVALIDATE:  //local and home node (shared)
            {
                if (WID(pwr->addr) == client->GetWorkerId()) {  //from home node, Case 4
                    epicAssert(!wr->counter);  //must be 0 since the home node has broadcast forward/invalidate req to other remote nodes
                    epicAssert(WRITE == pwr->op || WRITE_PERMISSION_ONLY == pwr->op);
                    pwr->counter = 0;
                } else {
                    pwr->counter--;
                }

                /*
                 * we cannot blindly erase the pending request for write
                 * as there may be valid responses later
                 */
                epicAssert(pwr->parent);
                epicAssert(pwr->parent->op == WLOCK);
                pwr->status = wr->status;
                pwr->parent->status = wr->status;  //put the error status

                if (pwr->counter == 0) {
                    //undo the directory/cache changes
                    if (WRITE == pwr->op) {
                        epicAssert(pwr->flag & CACHED);
                        //cache.lock(pwr->addr);
                        cache.ToInvalid(pwr->addr);
                        //cache.unlock(pwr->addr);
                    } else if (WRITE_PERMISSION_ONLY == pwr->op) {
                        epicAssert(pwr->flag & CACHED);
                        //cache.lock(pwr->addr);
                        cache.UndoShared(pwr->addr);
                        //cache.unlock(pwr->addr);
                    } else if (FETCH_AND_INVALIDATE == pwr->op) {
                        epicAssert(pwr->ptr == ToLocal(pwr->addr));
                        //directory.lock(pwr->ptr);
                        directory.UndoDirty(pwr->ptr);
                        //directory.unlock(pwr->ptr);
                    } else {  //INVALIDATE
                        epicAssert(pwr->ptr == ToLocal(pwr->addr));
                        //directory.lock(pwr->ptr);
                        directory.UndoShared(pwr->ptr);
                        //directory.unlock(pwr->ptr);
                    }

                    pwr->unlock();
                    //don't need to lock parent
                    if (IsLocal(addr)) {
                        directory.unlock(ToLocal(addr));
                    } else {
                        cache.unlock(addr);
                    }

                    --pwr->parent->counter;
                    epicAssert(pwr->parent->counter == 0);  //lock is guaranteed to be only one block
                    parent->unlock(); // unlock earlier
                    // Notify() should be called in the very last after all usage of parent,
                    // since the app thread may exit the function and release the memory of parent
                    Notify(pwr->parent);
                    pwr->parent = nullptr;

                    ProcessToServeRequest(pwr);
                    int ret = ErasePendingWork(wr->id);
                    epicAssert(ret);
                    delete pwr;
                    pwr = nullptr;
                } else {
                    pwr->unlock();
                    parent->unlock(); // unlock earlier
                    //don't need to lock parent
                    if (IsLocal(addr)) {
                        directory.unlock(ToLocal(addr));
                    } else {
                        cache.unlock(addr);
                    }
                }
                // parent->unlock(); // @wentian: originally here
                break;
            }
            case WRITE_FORWARD:  //home node
            case WRITE_PERMISSION_ONLY_FORWARD:  //home node (shouldn't happen)
            {
                void* laddr = ToLocal(pwr->addr);  //ToLocal(pwr->addr) != pwr->ptr as it is a forward msg
                //directory.lock(laddr);
                epicAssert(pwr->op == WRITE_FORWARD);
                epicAssert(IsLocal(pwr->addr));
                epicAssert(pwr->parent);
                epicAssert(pwr->pid == pwr->parent->id);
                DirEntry* entry = directory.GetEntry(ToLocal(pwr->addr));
                epicAssert(entry);
                epicAssert(directory.GetState(entry) == DIR_TO_DIRTY);
                directory.UndoDirty(entry);
                //directory.unlock(laddr);

                Client* lcli = FindClientWid(pwr->pwid);
                lcli->WriteWithImm(nullptr, nullptr, 0, pwr->pid);  //ack the ownership change

                pwr->unlock();
                epicAssert(IsLocal(addr));
                directory.unlock(laddr);
                epicAssert(!pwr->status);
                parent->unlock();
                delete pwr->parent;
                pwr->parent = nullptr;
                ProcessToServeRequest(pwr);
                //TODO: verify this
                //we blindly erase the pending wr whose counter may be non-zero
                //following replies will be ignored since it cannot find pending wr in the pending list
                //ANSWER: it's ok here, since we are sure that we only have one response for these two ops
                int ret = ErasePendingWork(wr->id);
                epicAssert(ret);
                delete pwr;
                pwr = nullptr;
                break;
            }
            case INVALIDATE_FORWARD:
            {
                epicLog(LOG_INFO, "invalidate_forward failed");
                epicAssert(IsLocal(pwr->addr));
                pwr->counter--;

                /*
                 * we cannot blindly erase the pending request for write
                 * as there may be valid responses later
                 */
                WorkRequest* parent = pwr->parent;
                epicAssert(parent);
                epicAssert((parent->flag & LOCKED) && (parent->flag & TRY_LOCK));
                pwr->status = wr->status;

                if (pwr->counter == 0) {
                    void* laddr = ToLocal(pwr->addr);
                    directory.UndoShared(laddr);

                    pwr->unlock();
                    epicAssert(IsLocal(addr));
                    directory.unlock(ToLocal(addr));

                    Client* pcli = FindClientWid(pwr->pwid);
                    parent->status = wr->status;  //put the error status
                    parent->op = WRITE_REPLY;
                    parent->counter = 0;
                    epicAssert(parent->counter == 0);
                    SubmitRequest(pcli, parent);
                    parent->unlock();
                    delete parent;
                    parent = nullptr;
                    pwr->parent = nullptr;

                    ProcessToServeRequest(pwr);
                    int ret = ErasePendingWork(wr->id);
                    epicAssert(ret);
                    delete pwr;
                    pwr = nullptr;
                } else {
                    pwr->unlock();
                    parent->unlock();
                    epicAssert(IsLocal(addr));
                    directory.unlock(ToLocal(addr));
                }
                break;
            }
            default:
                epicLog(LOG_WARNING,
                        "Unrecognized pending work request %d for WRITE_REPLY",
                        pwr->op);
                exit(-1);
                break;
        }
    } else {  //if not failed
        pwr->lock();
        pwr->counter += wr->counter;
        if (pwr->counter == 0) {
            pwr->flag |= REQUEST_DONE;
            pwr->unlock();
            ProcessPendingRequest(client, pwr);
        } else {
            pwr->unlock();
        }
    }
    delete wr;
    wr = nullptr;
}
