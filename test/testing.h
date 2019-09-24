#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <functional>

#include "../lib/str/strtools.h"
#include "../lib/var/var.h"

#include "database.h"


// this testing framework was inspired by:
//
// http://cpp-tip-of-the-day.blogspot.ca/2015/09/building-your-own-unit-testing-framework.html
//

struct TestItem_s
{
	std::string name;
	std::function<void()> test;
};

static int32_t testsPassed = 0;
static int32_t testsFailed = 0;

inline void incrPassed()
{
	++testsPassed;
}

inline void incrFailed()
{
	++testsFailed;
}

struct TestFail_s
{
	std::string expression;
	std::string file;
	int line;
	std::string message;

	TestFail_s(const char* expression, const char* file, const int line, const std::string message) :
		expression(std::string{ expression }),
		file(std::string{ file }),
		line(line),
		message(message)
	{
		incrFailed();
	}
};

// for debug logs that should return true, this will return a true if all true
inline bool testAllTrue(std::vector<cvar> &debugLog)
{
	return std::all_of(
		debugLog.begin(), 
		debugLog.end(), 
		[](bool test)
	{
		if (test)
			incrPassed();
		else
			incrFailed();
		return test;
	});
}

#define ASSERTMSG(condition, message) ((condition) ? incrPassed() : throw TestFail_s({#condition,__FILE__,__LINE__,message}))
#define ASSERT(condition) ((condition) ? incrPassed() : throw TestFail_s({#condition,__FILE__,__LINE__,""}))
#define ASSERTDEBUGLOG(conditions) (testAllTrue(conditions) ? true : throw TestFail_s({#conditions,__FILE__,__LINE__,"sub-test failed"}))

using Tests = std::vector<TestItem_s>;
using Fails = std::vector<TestFail_s>;


// test runner
inline Fails runTests(Tests &tests)
{
	using namespace std;

    // we need a global database object to runt he tests.
    auto database = new openset::db::Database();


	Fails failed;
	
	cout << "Running " << tests.size() << " test units" << endl;
	cout << "------------------------------------------------------" << endl;

	auto idx = 0;
	for (auto &t : tests)
	{
		++idx;
		//try
		{
			t.test();
			cout << "PASSED - #" << idx << " '" << t.name << "'" << endl;
		}
		/*catch (TestFail_s & caught)
		{
			cout << "FAILED - #" << idx << " '" << t.name << "'" << endl;
			cout << "         ASSERT(" << caught.expression << ")" << endl;
			cout << "         " << caught.file << " @ " << caught.line << endl;
			if (caught.message.length())
				cout << "         DETAIL: " << caught.message << endl;
			failed.emplace_back(caught);
		}*/
	}

	cout << "------------------------------------------------------" << endl;
	cout << "TESTS RAN    " << (testsPassed + testsFailed) << endl;
	cout << "TESTS PASSED " << testsPassed << endl;
	cout << "TESTS FAILED " << testsFailed << endl;

	return failed;
}
