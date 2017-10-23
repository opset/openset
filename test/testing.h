#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <functional>

#include "../lib/str/strtools.h"
#include "../lib/var/var.h"


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

	TestFail_s(const char* expression, const char* file, int line, std::string message) :
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
		return test;
	});
}

#define ASSERTMSG(condition, message) ((condition) ? incrPassed() : throw TestFail_s({#condition,__FILE__,__LINE__,message}))
#define ASSERT(condition) ((condition) ? incrPassed() : throw TestFail_s({#condition,__FILE__,__LINE__,""}))

using Tests = std::vector<TestItem_s>;
using Fails = std::vector<TestFail_s>;


// Allows inline code blocks to be indented with the C++ code
// this detects the indent level and pulls it out and returns
// a new string void of empty lines, tabs expanded and 
// de-indented so the pyql parser can process it
inline std::string fixIndent(std::string source)
{

	std::vector<string> res;

	auto parts = split(source, '\n');

	auto indent = -1;

	for (auto p : parts)
	{
		int t;
		// replace tabs
		while ((t = p.find('\t')) != -1)
		{
			p.erase(t, 1);
			p.insert(t, "    ");
		}

		// skip empty lines or lines with just whitespace
		if (trim(p, " ").size() == 0)
			continue;

		if (indent == -1)
		{
			indent = 0;
			for (auto i = 0; i < p.length(); ++i)
			{
				indent = i;
				if (p[i] != ' ')
					break;
			}
		}

		p = p.erase(0, indent);

		res.push_back(p);
	}

	std::string outputString;

	for (auto r : res)
		outputString += r + "\n";

	return outputString;
};


// test runner
inline Fails runTests(Tests &tests)
{
	using namespace std;

	Fails failed;
	
	cout << "Running " << tests.size() << " test units" << endl;
	cout << "------------------------------------------------------" << endl;

	auto idx = 0;
	for (auto &t : tests)
	{
		++idx;
		try
		{
			t.test();
			cout << "PASSED - #" << idx << " '" << t.name << "'" << endl;
		}
		catch (TestFail_s & caught)
		{
			cout << "FAILED - #" << idx << " '" << t.name << "'" << endl;
			cout << "         ASSERT(" << caught.expression << ")" << endl;
			cout << "         " << caught.file << " @ " << caught.line << endl;
			if (caught.message.length())
				cout << "         DETAIL: " << caught.message << endl;
			failed.emplace_back(caught);
		}
	}

	cout << "------------------------------------------------------" << endl;
	cout << "RAN    " << (testsPassed + testsFailed) << endl;
	cout << "PASSED " << testsPassed << endl;
	cout << "FAILED " << testsFailed << endl;
	
	return failed;
}
