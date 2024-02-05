CPP = g++ -std=c++11 -DNDEBUG
GAM_INCLUDE = -I../../include -I../ -I../../lib/libcuckoo/src
DB_INCLUDE = -I.. -I../bench -I../storage -I../txn -I../utils
INCLUDE = $(GAM_INCLUDE) $(DB_INCLUDE)
LIBS = ../../src/libgalloc.a -libverbs -lboost_filesystem -lboost_system -lboost_date_time -pthread -lboost_thread -lcityhash -lmemcached
CFLAGS += -O3 -DLOCK -DBACKOFF -ggdb #-DPROFILE -fsanitize=address



SRC = TpccRandomGenerator.cpp ../Meta.cpp ../txn/TransactionManager2PL.cpp ../txn/TransactionManagerST.cpp ../utils/Profiler.cpp
#OBJ = TpccBenchmarkMain.o TpccRandomGenerator.o ../Meta.o ../txn/TransactionManager2PL.o ../txn/TransactionManagerST.o ../utils/Profiler.o
OBJ =  TpccRandomGenerator.o ../Meta.o ../txn/TransactionManager2PL.o ../txn/TransactionManagerST.o ../utils/Profiler.o

#TpccBenchmarkMain.cpp TpccBenchmarkMain.o
$(OBJ): %.o : %.cpp *.h ../txn/*.h ../utils/*.h ../storage/*.h ../bench/*.h ../*.h
	$(CPP) $< $(CFLAGS) $(INCLUDE) -g -c -o $@
TpccBenchmarkMain.o: TpccBenchmarkMain.cpp
	$(CPP) $< $(CFLAGS) $(INCLUDE) -g -c -o $@
TpccServer.o: TpccServer.cpp
	$(CPP) $< $(CFLAGS) $(INCLUDE) -g -c -o $@
tpcc: $(OBJ)  TpccBenchmarkMain.o ../../src/libgalloc.a
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

tpcc_server:$(OBJ) TpccServer.o ../../src/libgalloc.a
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

all: tpcc tpcc_server
build: tpcc tpcc_server

clean:
	rm -rf tpcc tpcc_server *.o ../*.o ../bench/*.o ../storage/*.o ../txn/*.o ../utils/*.o
