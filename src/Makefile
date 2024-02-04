CPP = g++ -std=c++11 -O0 -ggdb
#-O3 -DNDEBUG -ggdb -O0 -DNDEBUG -ggdb
INCLUDE = -I../include -I../lib/libcuckoo/src -I../lib/libcuckoo/cityhash-1.1.1/src
#INCLUDE = -I../include -I../lib/libcuckoo/src -I../lib/libcuckoo/cityhash-1.1.1/src -I/users/zhongle/local/include
#LIBS = ../src/libgalloc.a -libverbs -lpthread ../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a /users/zhongle/local/lib/libboost_thread.so /users/zhongle/local/lib/libboost_system.so
LIBS = ./libgalloc.a -libverbs -lpthread  -lboost_thread -lboost_system -lcityhash -lmemcached
CFLAGS += -rdynamic
ifdef dht
	CFLAGS += -DDHT
endif

#../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a

SRC = gfunc.cc ae.cc client.cc server.cc worker.cc gallocator.cc master.cc tcp.cc worker_handle.cc anet.cc rdma.cc util.cc zmalloc.cc log.cc slabs.cc workrequest.cc cache.cc directory.cc local_request.cc remote_request.cc pending_request.cc MurmurHash.cc logging.cc
OBJ = gfunc.o ae.o client.o server.o worker.o gallocator.o master.o tcp.o worker_handle.o anet.o rdma.o util.o zmalloc.o log.o slabs.o workrequest.o cache.o directory.o remote_request.o local_request.o pending_request.o MurmurHash.o logging.o

libgalloc.a: $(OBJ)
	ar rcs $@ $^

libgalloc.so: $(SRC)
	$(CPP) $(CFLAGS) $(INCLUDE) -fPIC -shared -o $@ $^ $(LIBS)

$(OBJ): %.o : %.cc
	$(CPP) $< $(CFLAGS) $(INCLUDE) -g -c -o $@

benchmark: ../test/benchmark.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

memory_server: ../test/Memory_node.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

lock_test: ../test/lock_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

example: ../test/example.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

example-r: ../test/example-r.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

worker: ../test/worker_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

master: ../test/master_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

rw_test: ../test/rw_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

fence_test: ../test/fence_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

map_test: ../test/map_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

gfunc_test: ../test/gfunc_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)

all: libgalloc.a lock_test example example-r worker master rw_test fence_test benchmark map_test gfunc_test memory_server
test: libgalloc.a lock_test example example-r worker master rw_test fence_test benchmark map_test gfunc_test
build: libgalloc.a libgalloc.so

clean_test:
	rm -rf benchmark memory_server example worker master example-r rw_test fence_test lock_test

clean:
	rm -rf gfunc_test benchmark memory_server example worker master example-r rw_test fence_test lock_test map_test *.o *.a *.so *.d
