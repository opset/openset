#include "test_helper.h"

#include "testing.h"
#include "../lib/var/var.h"
#include "../src/database.h"
#include "../src/table.h"
#include "../src/asyncpool.h"
#include "../src/tablepartitioned.h"
#include "../src/queryparser.h"
#include "../src/queryparser2.h"
#include "../src/queryinterpreter.h"


openset::query::Interpreter* TestScriptRunner(const std::string& tableName, const std::string& script, openset::query::Macro_s& queryMacros, const bool debug)
{
    auto database = openset::globals::database;
    auto table    = database->getTable(tableName);
    auto parts    = table->getPartitionObjects(0, true); // partition zero for test

    openset::query::QueryParser2 p;

    // compile test script
        p.compileQuery(script, table->getColumns(), queryMacros, nullptr);
   
    if (debug)
        cout << openset::query::MacroDbg(queryMacros) << endl;

    ASSERT(p.error.inError() == false);

    auto interpreter = new openset::query::Interpreter(queryMacros);

    openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
    interpreter->setResultObject(&resultSet);

    const auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
    ASSERT(personRaw != nullptr);
    auto mappedColumns = interpreter->getReferencedColumns();

    // MappedColumns? Why? Because the basic mapTable function (without a
    // columnList) maps all the columns in the table - which is what we want when
    // inserting or updating rows but means more processing and less data affinity
    // when performing queries
    Person person; // Person overlay for personRaw;
    person.mapTable(table.get(), 0, mappedColumns);
    person.mount(personRaw); // this tells the person object where the raw compressed data is
    person.prepare();        // this actually decompresses

    // this mounts the now decompressed data (in the person overlay)
    // into the interpreter
    interpreter->mount(&person); // run it
    interpreter->exec();

    ASSERT(interpreter->error.inError() == false);
    return interpreter;
}
