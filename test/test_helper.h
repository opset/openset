#pragma once

#include "../src/queryinterpreter.h"
#include "../src/queryparser2.h"

openset::query::Interpreter* TestScriptRunner(const std::string& tableName, const std::string& script, openset::query::Macro_s& queryMacros, const bool debug = false);
