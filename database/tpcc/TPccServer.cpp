//
// Created by ruihong on 1/23/24.
//

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
    //TODO: use similar server code as the microbenchmark.
//    ArgumentsParser(argc, argv);
//
//    std::string my_host_name = ClusterHelper::GetLocalHostName();
//    ClusterConfig config(my_host_name, port, config_filename);
//    ClusterSync synchronizer(&config);
//    FillScaleParams(config);
//    PrintScaleParams();
//
//    TpccInitiator initiator(gThreadCount, &config);
//    // initialize GAM storage layer
//    initiator.InitGAllocator();
//    synchronizer.Fence();

    return 0;
}




