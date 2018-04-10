#pragma once

#include "include/libcommon.h"
#include "logger.h"

#include <iostream>
#include <chrono>
#include <memory>
#include <functional>
#include <tuple>

const int32_t PARTITION_MAX = 1024; // hard limit, not operating limit
const int32_t MAXCOLUMNS = 4096;

/*
	Because the full names a just do damn long and ugly turning what could
	usually fit on one line of code into two
*/
#define recast reinterpret_cast
#define cast static_cast

enum class serializedBlockType_e : int64_t
{
	attributes = 1,
	people = 2
};

/*
	These should be moved out, but I'm putting them here
	until I get a feel for how many of these there are
*/
int64_t Now();

int64_t MakeHash(const char* buffer, int64_t len);
int64_t MakeHash(const char* buffer);
int64_t MakeHash(const std::string& buffer);
int64_t HashPair(const int64_t a, const int64_t b);

int64_t AppendHash(int64_t value, int64_t last);
int64_t AppendHash(int32_t value, int64_t last);

void ThreadSleep(int64_t milliseconds);

using namespace std;

namespace std
{
	// hasher for std::pair<std::string, std::string>
	template <>
	struct hash<std::pair<std::string, std::string>>
	{
		size_t operator()(const std::pair<std::string, std::string>& v) const
		{
			return static_cast<size_t>(MakeHash(v.first + v.second));
		}
	};

	// hasher for std::pair<int64_t, int64_t>
	template <>
	struct hash<std::pair<int64_t, int64_t>>
	{
		size_t operator()(const std::pair<int64_t, int64_t>& v) const
		{
			return static_cast<size_t>(MakeHash(recast<const char*>(&v), sizeof(v)));
		}
	};

	// hasher for std::pair<int32_t, int64_t>
	template <>
	struct hash<std::pair<int32_t, int64_t>>
	{
		size_t operator()(const std::pair<int32_t, int64_t>& v) const
		{
			return static_cast<size_t>(MakeHash(recast<const char*>(&v), sizeof(v)));
		}
	};
    
    // hasher for std::pair<int64_t, int32_t>
	template <>
	struct hash<std::pair<int64_t, int32_t>>
	{
		size_t operator()(const std::pair<int32_t, int32_t>& v) const
		{
			return static_cast<size_t>(MakeHash(recast<const char*>(&v), sizeof(v)));
		}
	};

};

namespace std {
	namespace
	{
		// I borrowed this generic tuple hasher from StackOverflow:
		//
		// http://stackoverflow.com/questions/20834838/using-tuple-in-unordered-map
		//
		// Code from boost
		// Reciprocal of the golden ratio helps spread entropy
		//     and handles duplicates.
		// See Mike Seymour in magic-numbers-in-boosthash-combine:
		//     http://stackoverflow.com/questions/4948780

		template <class T>
		inline void hash_combine(std::size_t& seed, T const& v)
		{
			seed ^= hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
		}

		// Recursive template code derived from Matthieu M.
		template <class Tuple, size_t Index = std::tuple_size<Tuple>::value - 1>
		struct HashValueImpl
		{
			static void apply(size_t& seed, Tuple const& tuple)
			{
				HashValueImpl<Tuple, Index - 1>::apply(seed, tuple);
				hash_combine(seed, get<Index>(tuple));
			}
		};

		template <class Tuple>
		struct HashValueImpl<Tuple, 0>
		{
			static void apply(size_t& seed, Tuple const& tuple)
			{
				hash_combine(seed, get<0>(tuple));
			}
		};
	}

	template <typename ... TT>
	struct hash<std::tuple<TT...>>
	{
		size_t
			operator()(std::tuple<TT...> const& tt) const
		{
			size_t seed = 0;
			HashValueImpl<std::tuple<TT...> >::apply(seed, tt);
			return seed;
		}

	};
}

using voidfunc = std::function<void()>;
