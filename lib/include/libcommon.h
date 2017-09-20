#pragma once

#include <cstdlib>

// Commonly used standard includes

#include <cstdio>
#include <cstdlib>
#include <string>

#if defined( _MSC_VER )

#define NOMINMAX
#define MSG_NOSIGNAL 0

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Winsock2.h>
#include <Windows.h>

#include <cstdint>
#include <process.h>
#include <iostream>

#include "win32/types.h"

#else

#include <cstdint>

#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include <errno.h>

#include "linux/types.h"

#include <climits>

#endif

// Commonly used macros
#define SAFE_DELETE(x) { if(x != NULL) { delete(x); x = NULL; } }
#define SAFE_DELETE_STRING(x) { if(x != NULL) { delete [] x; x = NULL; } }
#define SAFE_DELETE_ARRAY(x) { if(x != NULL) { delete [] x; x = NULL; } }
#define SAFE_DELETE_VECTOR_POINTERS( type, vector ) { type* ptr; while ( ! vector.empty() ) { ptr = vector.back(); SAFE_DELETE( ptr ); vector.pop_back(); } }

const int64_t NULLCELL = LLONG_MIN; // huge negative number, allows for memset 0xFE to clear large structs
const int64_t NOVALUE = LLONG_MAX;
