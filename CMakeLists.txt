cmake_minimum_required(VERSION 3.6.0)
project(openset C CXX)

message(STATUS "OpenSet build mode is: ${CMAKE_BUILD_TYPE}")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug ... FORCE)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
	if (MSVC)
		add_definitions(/std:c++17)
	else()
		add_definitions(-m64 -std=c++17 -pthread -g -static-libstdc++ -static-libgcc)
	endif()

elseif(CMAKE_BUILD_TYPE MATCHES Release)
	if (MSVC)
		add_definitions(/std:c++17 /O2)
	else()
		add_definitions(-m64 -std=c++17 -pthread -g -static-libstdc++ -static-libgcc -msse2 -fexpensive-optimizations -ftree-vectorize -fprefetch-loop-arrays -funroll-loops)
	endif()
endif()

add_subdirectory(vendor/libuv-cmake)
include_directories(src lib vendor/libuv/include vendor/lz4/lib)

set(SOURCE_FILES
        vendor/libuv/include/uv.h
        vendor/lz4/lib/lz4.c
        vendor/lz4/lib/lz4.h
        vendor/lz4/lib/xxhash.c
        vendor/lz4/lib/xxhash.h
        src/common.cpp
        src/common.h
        lib/cjson/cjson.cpp
        lib/cjson/cjson.h
        lib/file/directory.cpp
        lib/file/directory.h
        lib/file/file.cpp
        lib/file/file.h
        lib/file/path.cpp
        lib/file/path.h
        lib/heapstack/heapstack.cpp
        lib/heapstack/heapstack.h
        lib/sba/sba.cpp
        lib/sba/sba.h
        lib/include/libcommon.h
        lib/mem/bigring.h
        lib/mem/bloom.cpp
        lib/mem/bloom.h
        lib/mem/prequeues.cpp
        lib/mem/prequeues.h
        lib/mem/ssdict.h
        lib/str/strtools.cpp
        lib/str/strtools.h
        lib/threads/spinlock.h
        lib/threads/event.cpp
        lib/threads/event.h
        lib/threads/locks.h
        lib/time/epoch.h
        lib/var/var.cpp
        lib/var/var.h
        src/asyncloop.cpp
        src/asyncloop.h
        src/asyncpool.cpp
        src/asyncpool.h
        src/attributeblob.cpp
        src/attributeblob.h
        src/attributes.cpp
        src/attributes.h
        src/columns.cpp
        src/columns.h
        src/config.cpp
        src/config.h
        src/database.cpp
        src/database.h
        src/dbtypes.h
        src/errors.cpp
        src/errors.h
        src/grid.cpp
        src/grid.h
        src/indexbits.cpp
        src/indexbits.h
        src/internodecommon.h
        src/internodemapping.cpp
        src/internodemapping.h
        src/internodemessage.cpp
        src/internodemessage.h
        src/internodeoutbound.cpp
        src/internodeoutbound.h
        src/internoderoute.cpp
        src/internoderoute.h
        src/internoderouter.cpp
        src/internoderouter.h
        src/logger.h
        src/main.cpp
        src/message_broker.cpp
        src/message_broker.h
        src/oloop.cpp
        src/oloop.h
        src/oloop_count.cpp
        src/oloop_count.h
        src/oloop_insert.cpp
        src/oloop_insert.h
        src/oloop_query.cpp
        src/oloop_query.h
        src/oloop_retrigger.cpp
        src/oloop_retrigger.h
        src/oloop_seg_refresh.cpp
        src/oloop_seg_refresh.h
        src/people.cpp
        src/people.h
        src/person.cpp
        src/person.h
        src/querycommon.h
        src/queryindexing.cpp
        src/queryindexing.h
        src/queryinterpreter.cpp
        src/queryinterpreter.h
        src/queryparser.cpp
        src/queryparser.h
        src/result.cpp
        src/result.h
        src/rpc.cpp
        src/rpc.h
		src/sentinel.h
        src/sentinel.cpp
        src/service.cpp
        src/service.h
        src/shuttle.h
        src/table.cpp
        src/table.h
        src/tablepartitioned.cpp
        src/tablepartitioned.h
        src/trigger.cpp
        src/trigger.h
        src/triggers.cpp
        src/triggers.h
        src/uvserver.cpp
        src/uvserver.h
        test/test_complex_events.h
        test/test_db.h
        test/test_lib_var.h
        test/test_pyql_language.h
        test/testing.h
        test/unittests.h
)

add_executable(openset ${SOURCE_FILES})

enable_testing()
add_test(NAME openset-unit-test COMMAND $<TARGET_FILE:openset> --test)

target_link_libraries(openset uv)
