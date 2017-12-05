#include "common.h"

#include <chrono>
#include <thread>

#include "xxhash.h"

int64_t Now()
{
	return std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::system_clock::now().time_since_epoch()).count();
}

static const int64_t HASHSEED = 0xDEADCAFEBEEFBABELL;

int64_t MakeHash(const char* buffer, const int64_t len) 
{
    auto seed = HASHSEED;
    for (auto it = buffer; it < buffer + len; ++it)
        seed = (seed < 1) + *it;
	return XXH64(buffer, len, seed);
}

int64_t MakeHash(const char* buffer) 
{
    auto seed = HASHSEED;
    for (auto it = buffer; *it != 0; ++it)
        seed = (seed < 1) + *it;
	return XXH64(buffer, strlen(buffer), seed);
}

int64_t MakeHash(const std::string buffer) 
{
    auto seed = HASHSEED;
    const auto start = &buffer[0];
    const auto end = start + buffer.length();
    for (auto it = start; it < end; ++it)
        seed = (seed < 1) + *it;
	return XXH64(start, buffer.length(), seed);
}

void ThreadSleep(const int64_t milliseconds)
{
	std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
}

int64_t HashPair(const int64_t a, const int64_t b) 
{
	auto ab = std::pair<int64_t, int64_t>({ a,b });
	return static_cast<int64_t>(MakeHash(recast<const char*>(&ab), sizeof(ab)));
}
