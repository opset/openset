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
    set_property(GLOBAL PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
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

include_directories(src lib vendor/lz4/lib vendor/asio/asio/include vendor/simple-web-server vendor/robin-hood-hashing)

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
        vendor/robin-hood-hashing/robin_hood.h
        src/common.cpp
        src/common.h
        src/customer.cpp
        src/customer.h
        src/customers.cpp
        src/customers.h
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
        lib/mem/bloom.cpp
        lib/mem/bloom.h
        lib/mem/ssdict.h
        lib/mem/blhash.h
        lib/mem/segmented_list.h
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
        src/config.cpp
        src/config.h
        src/customer_index.cpp
        src/customer_index.h
        src/customer_props.cpp
        src/customer_props.h
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
        src/oloop_customer.cpp
        src/oloop_customer.h
        src/oloop_customer_basic.cpp
        src/oloop_customer_basic.h
        src/oloop_customer_list.cpp
        src/oloop_customer_list.h
        src/oloop_histogram.cpp
        src/oloop_histogram.h
        src/oloop_insert.cpp
        src/oloop_insert.h
        src/oloop_property.cpp
        src/oloop_property.h
        src/oloop_query.cpp
        src/oloop_query.h
        src/oloop_segment.cpp
        src/oloop_segment.h
        src/oloop_seg_refresh.cpp
        src/oloop_seg_refresh.h
        src/properties.cpp
        src/properties.h
        src/property_mapping.cpp
        src/property_mapping.h
        src/querycommon.h
        src/queryindexing.cpp
        src/queryindexing.h
        src/queryinterpreter.cpp
        src/queryinterpreter.h
        src/queryparserosl.cpp
        src/queryparserosl.h
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
        src/rpc_query.cpp
        src/rpc_query.h
        src/rpc_sub.cpp
        src/rpc_sub.h
        src/rpc_status.cpp
        src/rpc_status.h
        src/rpc_table.cpp
        src/rpc_table.h
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
        test/test_osl_language.h
        test/test_sessions.h
        test/test_count_methods.h
        test/testing.h
        test/test_helper.h
        test/test_helper.cpp
        test/unittests.h
)

add_executable(openset ${SOURCE_FILES})

enable_testing()
add_test(NAME openset-unit-test COMMAND $<TARGET_FILE:openset> --test)

if (MSVC)
    target_link_libraries(openset ${CMAKE_THREAD_LIBS_INIT})
else()
    target_link_libraries(openset pthread ${CMAKE_THREAD_LIBS_INIT})
endif()


