#include "common.h"

#include <chrono>
#include <thread>
#include <random>

#include "xxhash.h"

int64_t Now()
{
	return std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::system_clock::now().time_since_epoch()).count();
}

static const int64_t HASHSEED = 0xDEADCAFEBEEFBABELL;

int64_t MakeHash(const char* buffer, const int64_t len) 
{
	return XXH64(buffer, len, HASHSEED);
}

int64_t MakeHash(const char* buffer) 
{
	return XXH64(buffer, strlen(buffer), HASHSEED);
}

int64_t MakeHash(const std::string& buffer) 
{
	return XXH64(buffer.c_str(), buffer.length(), HASHSEED);
}
int64_t AppendHash(const int64_t value, const int64_t last)
{
    return XXH64(reinterpret_cast<const void*>(&value), 8, last);
}

int64_t AppendHash(const int32_t value, const int64_t last)
{
    return XXH64(reinterpret_cast<const void*>(&value), 4, last);    
}

int64_t randomRange(const int64_t high, const int64_t low)
{
	std::default_random_engine rd;
    const std::uniform_int_distribution<int> someNum(low, high);    
    return someNum(rd);
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
