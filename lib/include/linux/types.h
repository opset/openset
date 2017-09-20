#pragma once

#ifndef _MSC_VER

#include "../libcommon.h"

#include <unistd.h>
#include <strings.h>
#include <memory.h>

#define WAIT_TIMEOUT        ETIMEDOUT
#define INFINITE            0xffffffff

#define INVALID_SOCKET      -1
#define SOCKET_ERROR        -1
#define SD_SEND             SHUT_RD
#define SD_RECEIVE          SHUT_WR
#define SD_BOTH             SHUT_RDWR

#define __forceinline       __attribute__((always_inline))

#define INT64_FORMAT        "%ld"

typedef int32_t             SOCKET;

inline void lltoa( int64_t val, char* dest )
{
    sprintf( dest, "%li", val );
}

inline int32_t stricmp( const char* s1, const char* s2 )
{
    return strcasecmp( s1, s2 );
}

#endif 

