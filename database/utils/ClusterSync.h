#ifndef __DATABASE_UTILS_CLUSTER_SYNCHRONIZER_H__
#define __DATABASE_UTILS_CLUSTER_SYNCHRONIZER_H__

#include "gallocator.h"
#include "ClusterConfig.h"

namespace Database {
class ClusterSync{
public:
  ClusterSync(ClusterConfig *config) : config_(config) {
    sync_key_ = 0;
  }

    void Fence_XALLNodes() {
        int SYNC_KEY = 16384;// a big enough number.
        int node_id = default_gallocator->GetID();
        int id;
//    STEPS = NUMOFBLOCKS/((no_thread - 1)*(100-shared_ratio)/100.00L + 1);
        default_gallocator->Put(SYNC_KEY + node_id, &node_id, sizeof(int));
        int no_node = config_->GetPartitionNum() + config_->GetMemoryNum();
        for (int i = 1; i <= no_node; i++) {
            default_gallocator->Get(SYNC_KEY + i, &id);
            epicAssert(id == i);
        }
    }
  void Fence_XComputes() {
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    bool *flags = new bool[partition_num];
    memset(flags, 0, sizeof(bool)*partition_num);
    this->MasterCollect<bool>(flags + partition_id, flags);
    this->MasterBroadcast<bool>(flags + partition_id);
    delete[] flags;
    flags = nullptr;
  }

  template<class T>
  void MasterCollect(T *send, T *receive) {
    T data;
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    if (config_->IsMaster()) {
      for (size_t i = 0; i < partition_num; ++i) {
        if (i != partition_id) {
          default_gallocator->Get(
              (uint64_t)(sync_key_ + i), &data);
          memcpy(receive + i, &data, sizeof(T));
        }
        else {
          memcpy(receive + i, send, sizeof(T));
        }
      }
    }
    else {
      default_gallocator->Put((uint64_t)
          (sync_key_ + partition_id), send, sizeof(T));
    }
    sync_key_ += partition_num;
  }

  template<class T>
  void MasterBroadcast(T *send) {
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    if (config_->IsMaster()) {
      default_gallocator->Put(
          (uint64_t)(sync_key_ + partition_id), send, sizeof(T));
    }
    else {
      const size_t master_partition_id = 0;
      default_gallocator->Get((uint64_t)
          (sync_key_ + master_partition_id), send);
    }
    sync_key_ += partition_num;
  }

private:
  ClusterConfig *config_;
  uint64_t sync_key_;
};
}

#endif
