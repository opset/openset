cmake_minimum_required(VERSION 3.6.0)
project(openset C CXX)

message(STATUS "OpenSet build mode is: ${CMAKE_BUILD_TYPE}")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug ... FORCE)
endif()

SET (USE_STANDALONE_ASIO ON)

if(CMAKE_BUILD_TYPE MATCHES Debug)
	if (MSVC)
		add_compile_options(/std:c++17 /Oi)
		add_definitions(/DASIO_STANDALONE /DUSE_STANDALONE_ASIO /D_CRT_SECURE_NO_WARNINGS)
	else()
		add_compile_options(-m64 -std=c++17 -g -static -static-libstdc++ -static-libgcc -pthread)
		add_definitions(-DASIO_STANDALONE -DUSE_STANDALONE_ASIO -pthread)
		set(THREADS_PREFER_PTHREAD_FLAG TRUE)
		set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
		find_package(Threads REQUIRED)
	endif()

elseif(CMAKE_BUILD_TYPE MATCHES Release)
	if (MSVC)
		add_compile_options(/std:c++17 /Ox)
		add_definitions(/DASIO_STANDALONE /DUSE_STANDALONE_ASIO /D_CRT_SECURE_NO_WARNINGS)
	else()
		add_compile_options(-m64 -std=c++17 -static -static-libstdc++ -static-libgcc -msse2 -o3 -flto -fexpensive-optimizations -ftree-vectorize -fprefetch-loop-arrays -funroll-loops -pthread)
		add_definitions(-DASIO_STANDALONE -DUSE_STANDALONE_ASIO -pthread)
		set(THREADS_PREFER_PTHREAD_FLAG TRUE)
		set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
		find_package(Threads REQUIRED)
	endif()

elseif(CMAKE_BUILD_TYPE MATCHES RelWithDebInfo)
	if (MSVC)
		add_compile_options(/std:c++17 /Ox /Debug /Zi )
		add_definitions(/DASIO_STANDALONE /DUSE_STANDALONE_ASIO /D_CRT_SECURE_NO_WARNINGS)
	else()
		add_compile_options(-m64 -std=c++17 -g -static -static-libstdc++ -static-libgcc -msse2 -o3 -flto -fexpensive-optimizations -ftree-vectorize -fprefetch-loop-arrays -funroll-loops -pthread)
		add_definitions(-DASIO_STANDALONE -DUSE_STANDALONE_ASIO -pthread)
		set(THREADS_PREFER_PTHREAD_FLAG TRUE)	
		set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
		find_package(Threads REQUIRED)
	endif()

endif()

include_directories(src lib vendor/lz4/lib vendor/asio/asio/include vendor/simple-web-server)

# file(GLOB_RECURSE ASIO_FILES vendor/asio/asio/include/*.hpp)

set(SOURCE_FILES
        vendor/lz4/lib/lz4.c
        vendor/lz4/lib/lz4.h
        vendor/lz4/lib/xxhash.c
        vendor/lz4/lib/xxhash.h
		vendor/asio/asio/include/asio.hpp
		vendor/simple-web-server/server_http.hpp
		vendor/simple-web-server/client_http.hpp
		vendor/simple-web-server/utility.hpp
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
		lib/var/varblob.h
		src/ver.h
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
		src/columnmapping.cpp
		src/columnmapping.h
        src/config.cpp
        src/config.h
        src/database.cpp
        src/database.h
        src/dbtypes.h
        src/errors.cpp
        src/errors.h
        src/grid.cpp
        src/grid.h
		src/http_serve.cpp
		src/http_serve.h
		src/http_cli.cpp
		src/http_cli.h
        src/indexbits.cpp
        src/indexbits.h
        src/internodecommon.h
        src/internodemapping.cpp
        src/internodemapping.h
        src/internoderouter.cpp
        src/internoderouter.h
        src/logger.h
        src/main.cpp
        src/message_broker.cpp
        src/message_broker.h
        src/oloop.cpp
        src/oloop.h
		src/oloop_cleaner.cpp
		src/oloop_cleaner.h
		src/oloop_column.cpp
		src/oloop_column.h
		src/oloop_histogram.cpp
        src/oloop_histogram.h
		src/oloop_insert.cpp
        src/oloop_insert.h
		src/oloop_person.cpp
		src/oloop_person.h
        src/oloop_query.cpp
        src/oloop_query.h
        src/oloop_segment.cpp
        src/oloop_segment.h
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
        src/rpc_global.cpp
        src/rpc_global.h
		src/rpc_insert.cpp
		src/rpc_insert.h
        src/rpc_internode.cpp
        src/rpc_internode.h
        src/rpc_cluster.cpp
        src/rpc_cluster.h
        src/rpc_trigger.cpp
        src/rpc_trigger.h
        src/rpc_table.cpp
        src/rpc_table.h
		src/rpc_status.cpp
		src/rpc_status.h
		src/rpc_query.cpp
		src/rpc_query.h
        src/rpc.cpp
        src/rpc.h
		src/sentinel.h
        src/sentinel.cpp
        src/service.cpp
        src/service.h
        src/shuttle.h
		src/sidelog.h
        src/table.cpp
        src/table.h
        src/tablepartitioned.cpp
        src/tablepartitioned.h
        test/test_db.h
        test/test_lib_var.h
        test/test_pyql_language.h
		test/test_sessions.h
		test/test_count_methods.h
        test/testing.h
        test/unittests.h
)

add_executable(openset ${SOURCE_FILES})
set_target_properties(openset PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)

enable_testing()
add_test(NAME openset-unit-test COMMAND $<TARGET_FILE:openset> --test)

if (MSVC)
	target_link_libraries(openset ${CMAKE_THREAD_LIBS_INIT})
else()
	target_link_libraries(openset pthread ${CMAKE_THREAD_LIBS_INIT})
endif()


