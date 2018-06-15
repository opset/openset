#pragma once

#include "testing.h"
#include "test_lib_var.h"
#include "test_db.h"
#include "test_pyql_language.h"
#include "test_zorder.h"
#include "test_sessions.h"
#include "test_count_methods.h"
#include "../src/logger.h"

bool unitTest()
{
	Tests allTests;

	Logger::get().suspendLogging(true); // turn off the default logger, reduce output noise

	// addes all the tests in a test_unit (test units are in the includes above)
	const auto add = [&allTests](Tests newTests) {
		allTests.insert(allTests.end(), newTests.begin(), newTests.end());
	};

	// add test for var.h
	add(test_lib_cvar());
	add(test_db());
	add(test_pyql_language());
	add(test_zorder());
	add(test_sessions());
    add(test_count_methods());

	return runTests(allTests).size() == 0; // true if zero
}