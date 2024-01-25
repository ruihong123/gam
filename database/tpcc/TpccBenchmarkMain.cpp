#include "TpccExecutor.h"
#include "TpccPopulator.h"
#include "TpccSource.h"
#include "TpccInitiator.h"
#include "TpccConstants.h"
#include "Meta.h"
#include "TpccParams.h"
#include "BenchmarkArguments.h"
#include "ClusterHelper.h"
#include "ClusterSync.h"
#include <iostream>

using namespace Database::TpccBenchmark;
using namespace Database;

void ExchPerfStatistics(ClusterConfig* config, 
    ClusterSync* synchronizer, PerfStatistics* s);
int main(int argc, char* argv[]) {
  ArgumentsParser(argc, argv);

//  my_host_name = ClusterHelper::GetLocalHostName();
  ClusterConfig config(my_host_name, port, compute_config_filename, memory_config_filename);
  ClusterSync synchronizer(&config);
  FillScaleParams(config);
  PrintScaleParams();

  TpccInitiator initiator(gThreadCount, &config);
  // initialize GAM storage layer
  initiator.InitGAllocator();
  synchronizer.Fence_XALLNodes();
//    synchronizer.Fence_XComputes();
  // initialize benchmark data
  GAddr storage_addr = initiator.InitStorage();
  synchronizer.MasterBroadcast<GAddr>(&storage_addr); 
  std::cout << "storage_addr=" << storage_addr << std::endl;
  StorageManager storage_manager;
  storage_manager.Deserialize(storage_addr, default_gallocator);

  // populate database
  INIT_PROFILE_TIME(gThreadCount);
  TpccPopulator populator(&storage_manager, &tpcc_scale_params);
  populator.Start();
  REPORT_PROFILE_TIME
  (gThreadCount);
    synchronizer.Fence_XComputes();

  // generate workload
  IORedirector redirector(gThreadCount);
  size_t access_pattern = 0;
  TpccSource sourcer(&tpcc_scale_params, &redirector, num_txn,
                     SourceType::PARTITION_SOURCE, gThreadCount, dist_ratio,
                     config.GetMyPartitionId());
  //TpccSource sourcer(&tpcc_scale_params, &redirector, num_txn, SourceType::RANDOM_SOURCE, gThreadCount, dist_ratio);
  sourcer.Start();
    synchronizer.Fence_XComputes();

  {
    // warm up
    INIT_PROFILE_TIME(gThreadCount);
    TpccExecutor executor(&redirector, &storage_manager, gThreadCount);
    executor.Start();
    REPORT_PROFILE_TIME(gThreadCount);
  }
    synchronizer.Fence_XComputes();

  {
    // run workload
    INIT_PROFILE_TIME(gThreadCount);
    TpccExecutor executor(&redirector, &storage_manager, gThreadCount);
    executor.Start();
    REPORT_PROFILE_TIME
    (gThreadCount);
    ExchPerfStatistics(&config, &synchronizer, &executor.GetPerfStatistics());
  }

  std::cout << "prepare to exit..." << std::endl;
    synchronizer.Fence_XComputes();
  std::cout << "over.." << std::endl;
  return 0;
}

void ExchPerfStatistics(ClusterConfig* config, 
    ClusterSync* synchronizer, PerfStatistics* s) {
  PerfStatistics *stats = new PerfStatistics[config->GetPartitionNum()];
  synchronizer->MasterCollect<PerfStatistics>(
      s, stats);
  synchronizer->MasterBroadcast<PerfStatistics>(stats);
  for (size_t i = 0; i < config->GetPartitionNum(); ++i) {
    stats[i].Print();
    stats[0].Aggregate(stats[i]);
  }
  stats[0].PrintAgg();
  delete[] stats;
  stats = nullptr;
}


