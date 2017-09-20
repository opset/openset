#pragma once

#ifdef _MSC_VER

#define INT64_FORMAT    "%lli"

typedef int32_t socklen_t;

/*inline int64_t atoll( const char* str )
{
    return _atoi64( str );
}

inline int64_t strtoll( const char *nptr, char **endptr, int base )
{
    return _strtoi64( nptr, endptr, base );
}

inline void lltoa( int64_t val, char* dest )
{
    _i64toa( val, dest, 10 );
}*/

#include <time.h>

#ifdef _MSC_VER
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
#else
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000ULL
#endif

#ifdef _MSC_VER
struct timezone
{
	int tz_minuteswest; /* minutes W of Greenwich */
	int tz_dsttime; /* type of dst correction */
};
#endif

extern int gettimeofday(struct timeval* tv, struct timezone* tz);

#endif
