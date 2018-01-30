#include "trigger.h"
#include "tablepartitioned.h"
#include "queryinterpreter.h"
#include "table.h"
#include "queryparser.h"
#include "indexbits.h"
#include "config.h"
#include "file/file.h"

using namespace openset::revent;

Revent::Revent(reventSettings_s* settings, openset::db::TablePartitioned* parts) :
	settings(settings),
	lastConfigVersion(settings->configVersion),
	parts(parts),
	interpreter(nullptr),
	person(nullptr),
	attr(nullptr), 
	bits(nullptr),
	currentFunctionHash(0),
	beforeState(false),
	inError(false)
{
	init();
}

/* compileTrigger is a static function called by the Triggers class. 
 *  
 *  Purpose is to compile a trigger so it can be shared amongst instances of the
 *  trigger class.
 */
openset::errors::Error Revent::compileTriggers(
	openset::db::Table* table, 
	const std::string &script,
	openset::query::Macro_s &targetMacros)
{
	openset::query::QueryParser p;	
	p.compileQuery(script.c_str(), table->getColumns(), targetMacros);	
	return p.error;
}

void Revent::init()
{
	// local copy of macros
	macros = settings->macros;

	if (interpreter)
	{
		flushDirty();
		delete interpreter;
		delete bits;
	}

	interpreter = new openset::query::Interpreter(macros, openset::query::InterpretMode_e::job);

	// This is the text value for this triggers on_insert event
	const auto valueName = settings->name + "::on_insert";

	settings->id = MakeHash(valueName);

	// get our index bits if we have any. We will be keeping these cached
	attr = parts->attributes.getMake(COL_TRIGGERS, valueName);
	bits = new IndexBits();
	bits->mount(attr->index, attr->ints, attr->linId);

	// this call back will be called by the 'schedule' marshal in the interpretor
	const auto schedule_cb = [&](int64_t functionHash, int seconds) -> bool
	{

		// clear it if it's already set
		this->person->getGrid()->clearFlag(
			openset::db::flagType_e::future_trigger,
			settings->id, // this is the trigger id
			functionHash); // context contains the function name

						   // add it
		auto newPerson = this->person->getGrid()->addFlag(
			openset::db::flagType_e::future_trigger,
			settings->id, // this is the trigger id
			functionHash,  // context is the function to call in the future
			Now() + seconds // value is future time this will run at
		);

		parts->people.replacePersonRecord(newPerson);

		return true;
	};

	interpreter->setScheduleCB(schedule_cb);

    /*
	// this call back will be called by the 'schedule' marshal in the interpretor
	auto emit_cb = [&](std::string emitMessage) -> bool
	{

		// flip some bits when we emits - these will get flushed by the 
		// standard dirty write back on insert
		parts->attributes.getMake(COL_EMIT, emitMessage);
		parts->attributes.addChange(COL_EMIT, MakeHash(emitMessage), person->getMeta()->linId, true);

		// queue trigger messages, these are held on a per trigger basis
		// these are shuttled out to a master queue at some point

		// TODO - shuttle these out to a master queue
		triggerQueue.emplace_back(
			triggerMessage_s{
			settings->id,
			emitMessage,
			person->getMeta()->getIdStr()
		});

		return true;
	};
    */

	//interpreter->setEmitCB(emit_cb);

	lastConfigVersion = settings->configVersion;
}

void Revent::flushDirty() 
{
	/*
	*  Unlike regular attributes, triggers keep a local uncompressed bit index
	*
	*  flushDirty takes the local bit index, compresses it, and injects it into
	*  the regular attributes index.
	*
	* 	The master attribute list must be maintained in order commit them or use
	* 	them in indexing.
	*/

	int64_t compBytes = 0; // OUT value via reference filled by ->store
	int64_t linId = -1;
	const auto compData = bits->store(compBytes, linId);

	auto attrPair = parts->attributes.columnIndex.find({ COL_TRIGGERS, settings->id }); // settings.id is this trigger

	const auto oldAttr = attrPair->second; // keep it so we can delete it.

	const auto newAttr = recast<Attr_s*>(
		PoolMem::getPool().getPtr(sizeof(Attr_s) + compBytes));

	// copy header
	std::memcpy(newAttr, oldAttr, sizeof(Attr_s));
	std::memcpy(newAttr->index, compData, compBytes);
	newAttr->linId = linId;

	// swap old index
	attrPair->second = newAttr;
	PoolMem::getPool().freePtr(oldAttr);

	// update our attr pointer
	attr = newAttr;
}

void Revent::mount(openset::db::Person* personPtr)
{
	if (inError)
		return;
	person = personPtr;
	interpreter->mount(person);
}

void Revent::preInsertTest()
{
	checkReload();

	if (inError)
		return;

	// check the index to see if this persons bit has been flipped
	beforeState = bits->bitState(person->getMeta()->linId);
}

void Revent::postInsertTest()
{
	checkReload();

	if (inError)
		return;

	if (beforeState)
		return; // already done it

	currentFunctionHash = settings->entryFunctionHash;
	interpreter->exec(settings->entryFunctionHash); // call the script 'trigger' function
    emit(settings->entryFunction);

    // this bit tells us we already ran this function
    if (!beforeState && interpreter->jobState)
        bits->bitSet(person->getMeta()->linId);
}

bool Revent::emit(const std::string& methodName)
{
    auto returns = interpreter->getLastReturn();

    if (!returns.size() || returns[0] == NONE)
        return false;

    const auto emitMessage = returns[0];

    // flip some bits when we emits - these will get flushed by the 
    // standard dirty write back on insert
    parts->attributes.getMake(COL_EMIT, emitMessage);
    parts->attributes.addChange(COL_EMIT, MakeHash(emitMessage), person->getMeta()->linId, true);

    // queue trigger messages, these are held on a per trigger basis
    // these are shuttled out to a master queue at some point

    // push to a local, lockless queue, they will be batched into
    // a central queue when oloop_revent is done a cycle
    triggerQueue.emplace_back(
        triggerMessage_s{
            settings->id,
            emitMessage,
            methodName,
            person->getMeta()->getIdStr()
        });

    return true;
}


bool Revent::runFunction(const int64_t functionHash)
{
	checkReload();

	currentFunctionHash = functionHash;
	interpreter->exec(functionHash);
    return emit(interpreter->calledFunction);
}

