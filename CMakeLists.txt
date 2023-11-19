cmake_minimum_required(VERSION 3.10)
project(GAM)

set(CMAKE_CXX_STANDARD 20)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
include_directories(
        "${PROJECT_SOURCE_DIR}/include"
        "${PROJECT_SOURCE_DIR}/database"
        "${PROJECT_SOURCE_DIR}/database/txn"
        "${PROJECT_SOURCE_DIR}/database/storage"
        "${PROJECT_SOURCE_DIR}/database/tpcc"
        "${PROJECT_SOURCE_DIR}/database/bench"
        "${PROJECT_SOURCE_DIR}/database/utils"
        "${PROJECT_SOURCE_DIR}/lib/libcuckoo/src"
        #        "${PROJECT_SOURCE_DIR}/third_party/googletest/googlemock"
        #        "${PROJECT_SOURCE_DIR}/third_party/googletest/googletest"
)
file(GLOB src_folder
        "${PROJECT_SOURCE_DIR}/src/*.h"
        "${PROJECT_SOURCE_DIR}/src/*.cc"
        "${PROJECT_SOURCE_DIR}/include/*.h"
)

file(GLOB TPCC_DEPENDENCY
        "${PROJECT_SOURCE_DIR}/database/*.h"
        "${PROJECT_SOURCE_DIR}/database/*.cpp"
        "${PROJECT_SOURCE_DIR}/database/storage/*.h"
        "${PROJECT_SOURCE_DIR}/database/bench/*.h"
        "${PROJECT_SOURCE_DIR}/database/tpcc/*.h"
        "${PROJECT_SOURCE_DIR}/database/tpcc/*.cpp"
        "${PROJECT_SOURCE_DIR}/database/util/*.cpp"
        "${PROJECT_SOURCE_DIR}/database/util/*.h"
)

add_library(GAM_Base ${src_folder} )
target_link_libraries(GAM_Base pthread)
add_executable(TPCC_CMAKE ${TPCC_DEPENDENCY} )
target_link_libraries(TPCC_CMAKE GAM_Base)
add_executable(Benchmark ${PROJECT_SOURCE_DIR}/test/benchmark.cc )
target_link_libraries(Benchmark GAM_Base)
add_custom_target(
        GAM
        COMMAND make -C ${PROJECT_SOURCE_DIR}/src
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/src
)


add_custom_target(
        TPCC
        COMMAND make tpcc
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/database/tpcc
)

add_executable(executable main.cpp)
target_link_libraries(executable ./src/libgalloc.a)
add_dependencies(executable GAM)

add_executable(memory_server test/Memory_node.cc)
target_link_libraries(executable ./src/libgalloc.a)