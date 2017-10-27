#include "trigger.h"
#include "tablepartitioned.h"
#include "queryinterpreter.h"
#include "table.h"
#include "queryparser.h"
#include "indexbits.h"
#include "config.h"
#include "file/file.h"

using namespace openset::trigger;

Trigger::Trigger(triggerSettings_s* settings, openset::db::TablePartitioned* parts) :
	settings(settings),
	table(parts->table),
	parts(parts),
	interpreter(nullptr),
	person(nullptr),
	attr(nullptr),
	bits(nullptr), 
	currentFunctionHash(0),
	runCount(0),
	beforeState(false),
	inError(false),
	lastConfigVersion(settings->configVersion)
{
	init();
}

Trigger::~Trigger() 
{}

/* compileTrigger is a static function called by the Triggers class. 
 *  
 *  Purpose is to compile a trigger so it can be shared amongst instances of the
 *  trigger class.
 */
openset::errors::Error Trigger::compileTrigger(
	openset::db::Table* table, 
	std::string name, 
	std::string script,
	openset::query::Macro_s &targetMacros)
{

/*
	auto fileName =
		cfg::manager->path + "tables/" + table->getName() +
		"/triggers/" + name + ".pyql";

	auto size = OpenSet::IO::File::FileSize(fileName.c_str());

	auto file = fopen(fileName.c_str(), "rb");

	if (file == nullptr)
	{
		OpenSet::errors::Error error;
		error.set(
			OpenSet::errors::errorClass_e::config,
			OpenSet::errors::errorCode_e::could_not_open_trigger,
			"could not open trigger '" + name + "' for table '" + table->getName() + "'." );
		return error;
	}

	auto data = recast<char*>(PoolMem::getPool().getPtr(size + 1));

	fread(data, 1, size, file);
	fclose(file);
	data[size] = 0;
*/

	openset::query::QueryParser p;	
	p.compileQuery(script.c_str(), table->getColumns(), targetMacros);	
	return p.error;
}

void Trigger::init()
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
	auto schedule_cb = [&](int64_t functionHash, int seconds) -> bool
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

	// this call back will be called by the 'schedule' marshal in the interpretor
	auto emit_cb = [&](std::string emitMessage) -> bool
	{

		// flip some bits when we emits - these will get flushed by the 
		// standard dirty write back on insert
		auto emitAttr = parts->attributes.getMake(5, emitMessage);
		emitAttr->addChange(person->getMeta()->linId, true);

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

	interpreter->setEmitCB(emit_cb);

	lastConfigVersion = settings->configVersion;

	/*Logger::get().info(
		' ', 
		"initialized trigger '" + settings->name + "' on table '" + table->getName() + 
		"' on partition " + to_string(parts->partition) + ".");
	*/
}

void Trigger::flushDirty() 
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
	int32_t linId = -1;
	const auto compData = bits->store(compBytes, linId);

	const auto columnIndex = parts->attributes.getColumnIndex(COL_TRIGGERS); // 4 is triggers
	auto attrPair = columnIndex->find(settings->id); // settings.id is this trigger

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

void Trigger::mount(openset::db::Person* personPtr)
{
	if (inError)
		return;
	person = personPtr;
	interpreter->mount(person);
}

void Trigger::preInsertTest()
{
	checkReload();

	if (inError)
		return;

	// check the index to see if this persons bit has been flipped
	beforeState = bits->bitState(person->getMeta()->linId);
}

void Trigger::postInsertTest()
{
	checkReload();

	if (inError)
		return;

	if (beforeState)
		return; // already done it

	currentFunctionHash = settings->entryFunctionHash;
	interpreter->exec(settings->entryFunctionHash); // call the script 'trigger' function

	if (!beforeState && interpreter->jobState)
		bits->bitSet(person->getMeta()->linId);
}

bool Trigger::runFunction(const int64_t functionHash)
{
	checkReload();

	currentFunctionHash = functionHash;
	interpreter->exec(functionHash);

	return interpreter->jobState;
}

