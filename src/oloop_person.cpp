#include "oloop_person.h"
#include "http_serve.h"
#include "table.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "person.h"
#include "cjson/cjson.h"

using namespace openset::async;

OpenLoopPerson::OpenLoopPerson(
    Shuttle<int>* shuttle, 
    openset::db::Table* table,
    const int64_t uuid) :
    OpenLoop(table->getName(), oloopPriority_e::realtime),
    shuttle(shuttle),
    table(table),
    uuid(uuid)
{}

void OpenLoopPerson::prepare() 
{}

void OpenLoopPerson::run()
{
    auto parts = table->getPartitionObjects(loop->partition);
    const auto personData = parts->people.getPersonByID(uuid);

    if (!personData) // no person, not found
    {
        shuttle->reply(
            http::StatusCode::client_error_bad_request,
            openset::errors::Error{
            openset::errors::errorClass_e::query,
            openset::errors::errorCode_e::item_not_found,
            "person could not be found"
        }.getErrorJSON()
        );
        suicide();
        return;
    }

    db::Person person; // Person overlay for personRaw;
    person.mapTable(table, loop->partition); // will throw in DEBUG if not called before mount
    person.mount(personData);
    person.prepare(); // this actually decompressed the record and populates the grid

    auto json = person.getGrid()->toJSON();
    auto jsonString = cjson::stringify(&json);

    shuttle->reply(
        http::StatusCode::success_ok,
        &jsonString[0], 
        jsonString.length());

    // were done
    suicide();
}

void OpenLoopPerson::partitionRemoved()
{
    shuttle->reply(
        http::StatusCode::client_error_bad_request,
        openset::errors::Error{
            openset::errors::errorClass_e::run_time,
            openset::errors::errorCode_e::partition_migrated,
            "please retry query"
        }.getErrorJSON()
    );
}
