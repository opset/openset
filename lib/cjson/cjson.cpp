/* 
-----------------------------------------------------------------
 cjson - Copyright 2015, Seth A. Hamilton

 refer to cjson.h for comments.
 -----------------------------------------------------------------
*/

#include "cjson.h"
#include <iomanip>
#include <cstdio>
#include "../file/file.h"
#include "../sba/sba.h"
#include <string>

/*
enum class utf8Masks_e : uint8_t
{
	is_utf     = 0b1000'0000,
	one_byte   = 0b1100'0000,
	two_byte   = 0b1110'0000,
	three_byte = 0b1111'0000,
	followon   = 0b1000'0000
};
*/

// Split - an std::string split function. 
// some of these should just be part of the stl by now.
// I'm sure the folks that write the stl would make a much
// nicer version than this one, but this one works!
void Split(const std::string& Source, char Token, std::vector<std::string>& result)
{
	result.clear(); // clear it just in case it is being reused

	if (!Source.length())
		return;

    const auto length = Source.length();
	size_t start = 0;
    auto end = Source.find(Token, 0);

    while (end != std::string::npos)
	{
		// removes empty splits 
		if (end - start == 0)
		{
			while (start != length && Source[start] == Token)
				start++;

			if ((end = Source.find(Token, start)) == std::string::npos)
				break;
		}

	    const auto diff = end - start;

		if (diff > 0)
			result.push_back(Source.substr(start, diff));

		start += diff + 1;
		end = Source.find(Token, start);
	}

	// anything stray at the end, if so append it.
	if (length - start > 0)
		result.push_back(Source.substr(start, length - start));

	if (result.size() == 0)
		result.push_back(Source);
}

/*
Member functions for cjson
*/

cjson::cjson() :
	mem(new HeapStack()),
	nodeType(Types_e::VOIDED),
	nodeName(nullptr),
	nodeData(nullptr),
	membersHead(nullptr),
	membersTail(nullptr),
	memberCount(0),
    scratchPad(nullptr),
	siblingPrev(nullptr),
	siblingNext(nullptr),
	parentNode(nullptr),
	//rootNode(this),
	selfConstructed(true)
{
    scratchPad = mem->newPtr(256);
	setName("__root__");
	setType(Types_e::OBJECT);    
}

cjson::cjson(Types_e docType):
	cjson()
{
	nodeType =docType;
};

cjson::cjson(const std::string &value, const Mode_e mode):
	cjson()
{
    if (mode == Mode_e::file)
	    fromFile(value, this);
    else
        parse(value, this, true);
};

cjson::cjson(HeapStack* mem) :
    mem(mem),
    nodeType(Types_e::VOIDED),
    nodeName(nullptr),
    nodeData(nullptr),
    membersHead(nullptr),
    membersTail(nullptr),
    memberCount(0),
    scratchPad(nullptr),
    siblingPrev(nullptr),
    siblingNext(nullptr),
    parentNode(nullptr),
    selfConstructed(false)
{
    scratchPad = mem->firstBlock()->data;
}

cjson::cjson(cjson&& other) noexcept :
	mem(other.mem),
	nodeType(other.nodeType),
	nodeName(other.nodeName),
	nodeData(other.nodeData),
	membersHead(other.membersHead),
	membersTail(other.membersTail),
	memberCount(other.memberCount),
    scratchPad(other.scratchPad),
	siblingPrev(other.siblingPrev),
	siblingNext(other.siblingNext),
	parentNode(other.parentNode),
	selfConstructed(other.selfConstructed)
{
	other.selfConstructed = false;
	other.mem = nullptr;
	other.membersHead = nullptr;
	other.membersTail = nullptr;
    other.scratchPad = nullptr;
	other.siblingNext = nullptr;
	other.siblingPrev = nullptr;
	other.parentNode = nullptr;
	other.nodeType = Types_e::VOIDED;
};

cjson::~cjson()
{
	// this destructor does nothing and is never
	// called because we are using "placement new" for
	// our allocations
	if (selfConstructed)
		delete mem;
}

cjson& cjson::operator=(cjson&& other) noexcept
{
	mem = other.mem;
	nodeType = other.nodeType;
	nodeName = other.nodeName;
	nodeData = other.nodeData;
	membersHead = other.membersHead;
	membersTail = other.membersTail;
	memberCount = other.memberCount;
    scratchPad = other.scratchPad;
	siblingPrev = other.siblingPrev;
	siblingNext = other.siblingNext;
	parentNode = other.parentNode;
	//rootNode = this;//other.rootNode;
	selfConstructed = other.selfConstructed;

	other.selfConstructed = false;
	other.mem = nullptr;
	other.membersHead = nullptr;
	other.membersTail = nullptr;
    other.scratchPad = nullptr;
	other.siblingNext = nullptr;
	other.siblingPrev = nullptr;
	other.parentNode = nullptr;
	//other.rootNode = nullptr;
	other.nodeType = Types_e::VOIDED;

	return *this;
};

cjson::Types_e cjson::type() const
{
	return nodeType;
};

void cjson::setName(const char* newName)
{
	if (newName)
	{
	    const auto len = strlen(newName) + 1;
		this->nodeName = mem->newPtr(len);
		memcpy(this->nodeName, newName, len);
	}
};

void cjson::setName(const std::string& newName)
{
	setName(newName.c_str());
};

void cjson::setType(const Types_e type)
{
	nodeType = type;
}

cjson* cjson::createNode() const
{
	// using placement new to put object in HeapStack
	auto nodePtr = mem->newPtr(sizeof(cjson));
	return new(nodePtr) cjson(mem);
};

cjson* cjson::createNode(const Types_e type, const char* name) const
{
    auto newNode = createNode();
	newNode->setName(name);
	newNode->nodeType = type;
	return newNode;
};

cjson* cjson::createNode(const Types_e type, const std::string& name) const
{
    auto newNode = createNode();
	newNode->setName(name.c_str());
	newNode->nodeType = type;
	return newNode;
};

void cjson::removeNode()
{
	nodeType = Types_e::VOIDED;
	nodeName = nullptr;
	nodeData = nullptr;
	membersHead = nullptr;
	membersTail = nullptr;
	memberCount = 0;
};

cjson* cjson::hasMembers() const
{
	return membersHead;
}

cjson* cjson::hasParent() const
{
	return parentNode;
}

cjson::curs cjson::cursor()
{
    const curs newIter(this);
	return newIter;
}

bool cjson::isNode(const std::string& xPath) const
{
	return (getNodeByPath(xPath)) ? true : false;
};

bool cjson::hasName() const
{
	return (nodeName && *static_cast<char*>(nodeName) != 0) ? true : false;
};

std::vector<std::string> cjson::getKeys() const
{
	// allocate a list of the required size for all the node names
    const auto count = this->size();
	std::vector<std::string> names;
	names.reserve(count);

	auto n = membersHead;

	while (n)
	{
		if (n->nodeName && n->nodeType != Types_e::VOIDED)
			names.emplace_back(n->nodeName);
		n = n->siblingNext;
	}

	return names;
}

std::vector<cjson*> cjson::getNodes() const
{
	const auto count = this->size();
	std::vector<cjson*> array;
	array.reserve(count);

	// only return for ARRAY and OBJECT type nodes
	if (nodeType != Types_e::ARRAY &&
		nodeType != Types_e::OBJECT)
		return array; // return an empty array

	auto n = membersHead;

	while (n)
	{
		if (n->nodeType != Types_e::VOIDED)
			array.push_back(n);
		n = n->siblingNext;
	}

	return array;
}

cjson* cjson::at(const int index) const
{
	// linked lists don't do random access
	// so this won't be the fasted function ever written
	//
	// if repeated iteration is needed using one of the 
	// array type functions would probably be best
	//
	// i.e. GetNodes();

    auto iter = 0;
	auto n = membersHead;

	while (n)
	{
		if (iter == index)
			return n;

		n = n->siblingNext;
		iter++;
	}

	return nullptr;
};

cjson* cjson::find(const char* name) const
{
	auto n = membersHead;

	while (n)
	{
		if (n->nodeName && strcmp(n->nodeName, name) == 0)
			return n;

		n = n->siblingNext;
	}

	return nullptr;
}

int cjson::getIndex() const
{
	auto n = this->siblingPrev;

	if (!n)
		return 0;

	while (true) // rewind
	{
		if (!n->siblingPrev)
			break;
		n = n->siblingPrev;
	}

	auto idx = 0;

	while (n)
	{
		if (n == this)
			return idx;

		idx++;
		n = n->siblingNext;
	}

	return idx;
}

cjson* cjson::find(const std::string& name) const
{
	return find(name.c_str());
}

cjson* cjson::findByHashValue(const size_t hashedId) const
{
	auto n = membersHead;

    const hash<cjson> hasher;

	while (n)
	{
	    const auto hashId = hasher(*n);
		if (hashId == hashedId)
			return n;

		n = n->siblingNext;
	}
	return nullptr;
}

// helper, append a value to an array
cjson* cjson::push(const int64_t value)
{
	auto newNode = createNode();
	// initialize the node
	newNode->replace(value);
	link(newNode);
	return newNode;
}

// helper, append a value to an array
cjson* cjson::push(const double value)
{
	auto newNode = createNode();
	// initialize the node
	newNode->replace(value);
	link(newNode);
	return newNode;
};

// helper, append a value to an array
cjson* cjson::push(const char* value)
{
	auto newNode = createNode();
	// initialize the node
	newNode->replace(value);
	link(newNode);
	return newNode;
};

// helper, append a value to an array
cjson* cjson::push(const std::string& value)
{
	auto newNode = createNode();
	// initialize the node
	newNode->replace(value);
	link(newNode);
	return newNode;
};

// helper, append a value to an array
cjson* cjson::push(const bool value)
{
	auto newNode = createNode();
	// initialize the node
	newNode->replace(value);
	link(newNode);
	return newNode;
};

// helper, append a value to an array
cjson* cjson::push(cjson* node)
{
	link(node);
	return node;
};

// append a nested array onto the array
cjson* cjson::pushArray()
{
	cjson* newNode = createNode();
	// initialize the node
	newNode->nodeType = Types_e::ARRAY;

	link(newNode);
	return newNode;
};

// append a nested array onto the array
cjson* cjson::pushNull()
{
	auto newNode = createNode();

    // initialize the node
	newNode->nodeType = Types_e::NUL;

    link(newNode);
	return newNode;
};

// append a nested document onto the array
cjson* cjson::pushObject()
{
	auto newNode = createNode();

	// initialize the node
	newNode->nodeType = Types_e::OBJECT;

	link(newNode);
	return newNode;
};

// adds or updates an existing key value pair in a doc type node
cjson* cjson::set(const char* key, const int64_t value)
{
	auto node = find(key);

	if (!node)
	{
		auto newNode = createNode();

		newNode->setName(key);
		link(newNode);

		node = newNode;
	}

	node->replace(value);
	return node;
};

cjson* cjson::set(const std::string& key, const int64_t value)
{
	return set(key.c_str(), value);
}

cjson* cjson::set(const char* key, const int32_t value)
{
	return set(key, static_cast<int64_t>(value));
};

cjson* cjson::set(const std::string& key, const int32_t value)
{
	return set(key, static_cast<int64_t>(value));
}

// adds or updates an existing key value pair in a doc type node
cjson* cjson::set(const char* key, const double value)
{
	auto node = find(key);

	if (!node)
	{
		auto newNode = createNode();

		newNode->setName(key);

		link(newNode);

		node = newNode;
	}

	node->replace(value);
	return node;
};

cjson* cjson::set(const std::string& key, const double value)
{
	return set(key.c_str(), value);
}

// adds or updates an existing key value pair in a doc type node
cjson* cjson::set(const char* key, const char* value)
{
	auto node = find(key);

	if (!node)
	{
		auto newNode = createNode();

		newNode->setName(key);
		link(newNode);

		node = newNode;
	}

	node->replace(value);
	return node;
}

cjson* cjson::set(const std::string& key, const char* value)
{
	return set(key.c_str(), value);
};

cjson* cjson::set(const std::string& key, const std::string& value)
{
	return set(key.c_str(), value.c_str());
};

cjson* cjson::set(const char* key, const bool value)
{
	auto node = find(key);

	if (!node)
	{
	    auto newNode = createNode();

		newNode->setName(key);
		link(newNode);

		node = newNode;
	}

	node->replace(value);
	return node;
};

cjson* cjson::set(const std::string& key, const bool value)
{
	return set(key.c_str(), value);
};

// adds null
cjson* cjson::set(const char* key)
{
	auto node = find(key);

	if (!node)
	{
		auto newNode = createNode();

		newNode->setName(key);
		link(newNode);
		node = newNode;
	}

	node->nodeType = Types_e::NUL;
	return node;
}

cjson* cjson::set(const std::string& key)
{
	return set(key.c_str());
}

// adds or updates an existing key value pair in a doc type node

// adds a new array or returns an existing array key value type
cjson* cjson::setArray(const char* key)
{
    const auto node = find(key);

	if (!node)
	{
		auto newNode = createNode();

		newNode->setName(key);
		newNode->nodeType = Types_e::ARRAY;
		link(newNode);

		return newNode;
	}

	return node;
};

cjson* cjson::setArray(const std::string& key)
{
	return setArray(key.c_str());
};

// adds a new or returns an existing document key value type
cjson* cjson::setObject(const char* key)
{
    const auto node = find(key);

	if (!node)
	{
		auto newNode = createNode();

		newNode->setName(key);
		newNode->nodeType = Types_e::OBJECT;
		link(newNode);

		return newNode;
	}

	return node;
}

cjson* cjson::setObject(const std::string& key)
{
	return setObject(key.c_str());
}

// get number of items in an array or object
int cjson::size() const
{
	return memberCount;
}

bool cjson::empty() const
{
	return (memberCount == 0);
}

void cjson::replace(const int64_t val)
{
	nodeType = Types_e::INT;
	nodeData = reinterpret_cast<dataUnion*>(mem->newPtr(sizeof(val)));
	nodeData->asInt = val;
}

void cjson::replace(const double val)
{
	nodeType = Types_e::DBL;
	nodeData = reinterpret_cast<dataUnion*>(mem->newPtr(sizeof(val)));
	nodeData->asDouble = val;
}

void cjson::replace(const char* val)
{
	nodeType = Types_e::STR;

    const auto len = strlen(val) + 1;
    const auto textPtr = mem->newPtr(len);
	// we are going to copy the string to textPtr
	// but we have to point nodeData to this as well
	nodeData = reinterpret_cast<dataUnion*>(textPtr);
	memcpy(textPtr, val, len);
}

void cjson::replace(const std::string& val)
{
	replace(val.c_str());
}

void cjson::replace()
{
	nodeType = Types_e::NUL;
	nodeData = nullptr;
}

void cjson::replace(const bool val)
{
	nodeType = Types_e::BOOL;
	nodeData = reinterpret_cast<dataUnion*>(mem->newPtr(sizeof(val)));
	nodeData->asBool = val;
}

int64_t cjson::xPathInt(const std::string& path, const int64_t defaultValue) const
{
    const auto n = getNodeByPath(path);
	if (n && n->nodeType == Types_e::INT)
		return n->nodeData->asInt;
	return defaultValue;
}

bool cjson::xPathBool(const std::string& path, const bool defaultValue) const
{
    const auto n = getNodeByPath(path);
	if (n && n->nodeType == Types_e::BOOL)
		return n->nodeData->asBool;
	return defaultValue;
}

double cjson::xPathDouble(const std::string& path, const double defaultValue) const
{
    const auto n = getNodeByPath(path);
	if (n && n->nodeType == Types_e::DBL)
		return n->nodeData->asDouble;
	return defaultValue;
}

const char* cjson::xPathCstr(const char* path, const char* defaultValue) const
{
	const auto n = getNodeByPath(path);
	if (n && n->nodeType == Types_e::STR)
		return &n->nodeData->asStr;
	return defaultValue;
}

std::string cjson::xPathString(const std::string& path, const std::string& defaultValue) const
{
    const auto res = xPathCstr(const_cast<char*>(path.c_str()), defaultValue.c_str());
	return std::string(res);
}

cjson* cjson::xPath(const std::string& path) const
{
	return getNodeByPath(path);
};

std::string cjson::xPath() const
{
	std::string path;

	auto n = this;

	while (n)
	{
		if (n != this)
			path = ('/' + path);

	    const auto name = n->nameCstr();

		if (name && *name) // is there a name?
		{
			if (strcmp(name, "__root__") == 0)
				break;

			path += path;
		}
		else // no name means it's an array item
		{
			path = std::to_string(n->getIndex()) + path;
		}

		n = n->parentNode;
	}

	if (!path.length())
		return "/";

	return path;
}

// helpers for serialization
std::string cjson::name() const
{
	if (nodeName)
		return std::string(static_cast<char*>(nodeName));
	else
		return std::string("");
};

const char* cjson::nameCstr() const
{
	if (nodeName)
		return static_cast<const char*>(nodeName);
	else
		return nullptr;
};

bool cjson::isStringCstr(char*& value) const
{
	if (nodeType == Types_e::STR)
	{
		value = reinterpret_cast<char*>(nodeData);
		return true;
	}
	return false;
};

bool cjson::isString(std::string& value) const
{
	if (nodeType == Types_e::STR)
	{
		value = reinterpret_cast<char*>(nodeData);
		return true;
	}
	return false;
};

bool cjson::isInt(int64_t& value) const
{
	if (nodeType == Types_e::INT)
	{
		value = nodeData->asInt;
		return true;
	}
	return false;
};

bool cjson::isDouble(double& value) const
{
	if (nodeType == Types_e::DBL)
	{
		value = nodeData->asDouble;
		return true;
	}
	return false;
};

bool cjson::isBool(bool& value) const
{
	if (nodeType == Types_e::BOOL)
	{
		value = nodeData->asBool;
		return true;
	}
	return false;
};

bool cjson::isNull() const
{
	if (nodeType == Types_e::NUL)
		return true;
	return false;
}

int64_t cjson::getInt() const
{
	return (nodeData) ? nodeData->asInt : 0;
}

double cjson::getDouble() const
{
	return (nodeData) ? nodeData->asDouble : 0;
}

const char* cjson::getCstr() const
{
	return (nodeData) ? reinterpret_cast<const char*>(nodeData) : nullptr;
}

string cjson::getString() const
{
	return (nodeData) ? std::string(reinterpret_cast<const char*>(nodeData)) : ""s;
}

bool cjson::getBool() const
{
	return (nodeData) ? nodeData->asBool : false;
};

cjson* cjson::parse(const char* json, cjson* root, const bool embedded)
{
	auto cursor = const_cast<char*>(json);
	return cjson::parseBranch(root, cursor, embedded);
}

cjson* cjson::parse(const std::string& json, cjson* root, const bool embedded)
{
	return cjson::parse(json.c_str(), root, embedded);
};

char* cjson::stringifyCstr(const cjson* doc, int64_t& length, const bool pretty)
{
	HeapStack mem;	
	doc->stringify_worker(doc, mem, (pretty) ? 0 : -1, doc);
	
	const auto end = mem.newPtr(1);
	*end = 0; // stick a null at the end of that string

	length = mem.getBytes()-1;
	return mem.flatten();
}

HeapStack* cjson::stringifyHeapStack(cjson* doc, int64_t& length, const bool pretty)
{
    const auto mem = new HeapStack();

    doc->stringify_worker(doc, *mem, (pretty) ? 0 : -1, doc);

    // no null at the end
	return mem;
}


std::string cjson::stringify(cjson* doc, const bool pretty)
{
	HeapStack mem;

	doc->stringify_worker(doc, mem, (pretty) ? 0 : -1, doc);
    const auto end = mem.newPtr(1);
	*end = 0; // stick a null at the end of that string

	char* cstr;

	if (mem.getBlocks() == 1)
	{
		cstr = mem.getHeadPtr();
		return std::string(cstr);
	}
	else
	{
		cstr = mem.flatten();
		std::string result(cstr);
		mem.releaseFlatPtr(cstr);
		return result;
	}
}

void cjson::releaseStringifyPtr(char* strPtr)
{
	PoolMem::getPool().freePtr(strPtr);
}

cjson* cjson::makeDocument()
{
	auto mem = new HeapStack();
    mem->newPtr(256);
	auto data = mem->newPtr(sizeof(cjson));

	// we are going to allocate this node using "placement new"
	// in the HeapStack (in the heapstack it's about to own)
	auto newNode = new(data) cjson(mem);

	newNode->setName("__root__");
	newNode->setType(Types_e::OBJECT);
    newNode->selfConstructed = true; // so it will delete itself

	return newNode;
};

cjson* cjson::fromFile(const std::string& fileName, cjson* root)
{
    const auto size = openset::IO::File::FileSize(const_cast<char*>(fileName.c_str()));
    const auto file = fopen(fileName.c_str(), "rbe");

	if (file == nullptr)
		return nullptr;

    // file size plus a null terminator
	auto data = new char[size + 1];

	fread(data, 1, size, file);
	fclose(file);

	data[size] = 0; // null terminator

	if (!root)
	{
	    const auto doc = cjson::parse(data);
		delete[]data;
		return doc;
	}
	else
	{
		parseBranch(root, data);
		delete[]data;
		return root;
	}
}


bool cjson::toFile(const std::string& fileName, cjson* root, bool pretty)
{
	auto jText = cjson::stringify(root, pretty);

    const auto file = fopen(fileName.c_str(), "wbe");

	if (file == nullptr)
		return false;

	fwrite(jText.c_str(), 1, jText.length(), file);

	fclose(file);
	return true;
}


cjson* cjson::getNodeByPath(const std::string& path) const 
{
	auto n = this;

	std::vector<std::string> parts;
	Split(path, '/', parts);

	for (auto & part : parts)
	{
		switch (n->nodeType)
		{
			case Types_e::OBJECT:
				{
				    const auto next = n->find(part);

					if (!next)
						return nullptr;

					n = next;
				}
				break;
			case Types_e::ARRAY:
				{
                    char* endp;
				    const auto index = strtoll(part.c_str(), &endp, 10);

					if (index < 0 || index >= n->size())
						return nullptr;

					n = n->at(index);

					if (!n)
						return nullptr;
				}
				break;
			default: break;
		}
	}

	return const_cast<cjson*>(n);
}

size_t cjson::hashed() const
{
    const hash<cjson> hasher;
	return hasher(*this);
}

// internal add member
void cjson::link(cjson* newNode)
{
	newNode->parentNode = this;

	// members are basically children directly owned
	// by the current node. They are stored as a linked
	// list maintained by the parent node.
	// nodes can also see their siblings via siblingPrev and
	// siblingNext

	if (!membersHead)
	{
		membersHead = newNode;
		membersTail = newNode;
		newNode->siblingNext = nullptr;
		newNode->siblingPrev = nullptr;
	}
	else
	{
	    const auto lastTail = membersTail;
		// set the current tails sibling to the node		
		membersTail->siblingNext = newNode;
		// new nodes previous is the last tail
		newNode->siblingPrev = lastTail;
		// set the current tail to the node;
		membersTail = newNode;
	}

	memberCount++;
};

// local 
inline void emitIndent(HeapStack& writer, int indent)
{
	if (writer.getBytes()) // are we NOT first line in output
	{
	    const auto output = writer.newPtr(1);
		*output = '\n';
	}

    const auto len = indent * 4; // 4 space pretty print (matches jsonlint.com)
    const auto output = writer.newPtr(len);
	memset(output, ' ', len);
}

// local - helper for Stringify_worder
inline void emitText(HeapStack& writer, const char* text)
{
	const auto len = strlen(text);
	const auto output = writer.newPtr(len);
	memcpy(output, text, len);
}

// local - helper for Stringify_worker
inline void emitText(HeapStack& writer, const char text)
{
    const auto output = writer.newPtr(1);
	*output = text;
}

void cjson::stringify_worker(const cjson* n, HeapStack& writer, int indent, const cjson* startNode) const
{
	std::string pad = "";
	std::string padshort = "";

	switch (n->nodeType)
	{
		case Types_e::NUL:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (n->hasName())
			{
				emitText(writer, '"');
				emitText(writer, n->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			emitText(writer, "null");
			break;

		case Types_e::INT:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (n->hasName())
			{
				emitText(writer, '"');
				emitText(writer, n->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}
            
			snprintf(n->scratchPad, 128, "%lld", static_cast<long long int>(n->nodeData->asInt));
			emitText(writer, n->scratchPad);
			//emitText(writer, std::to_string(N->nodeData->asInt).c_str()); // slower pure C++ way
			break;

		case Types_e::DBL:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (n->hasName())
			{
				emitText(writer, '"');
				emitText(writer, n->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			if (n->nodeData->asDouble == 0)
			{
				emitText(writer, "0.0"); // emit value with float type - helps recipient manage type discovery
			}
			else
			{
				snprintf(n->scratchPad, 128, "%0.7f", n->nodeData->asDouble);
				emitText(writer, n->scratchPad);
				//emitText(writer, std::to_string(N->nodeData->asDouble).c_str()); // slower pure C++ way
			}
			break;

		case Types_e::STR:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (n->hasName())
			{
				emitText(writer, '"');
				emitText(writer, n->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			emitText(writer, '"');

			// we are going to emit any UTF-8 or other bytes as such
			{
				auto ch = reinterpret_cast<char*>(n->nodeData);
				while (*ch)
				{
					switch (*ch)
					{
						case '\r':
							emitText(writer, "\\r");
							break;
						case '\n':
							emitText(writer, "\\n");
							break;
						case '\t':
							emitText(writer, "\\t");
							break;
						case '\\':
							emitText(writer, "\\\\");
							break;
						case '\b':
							emitText(writer, "\\b");
							break;
						case '\f':
							emitText(writer, "\\f");
							break;
						case '"':
							emitText(writer, "\\\"");
							break;
						default:
							emitText(writer, *ch);
					}
					++ch;
				}
			}
			emitText(writer, '"');
			break;

		case Types_e::BOOL:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (n->hasName())
			{
				emitText(writer, '"');
				emitText(writer, n->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			emitText(writer, (n->nodeData->asBool) ? "true" : "false");
			break;

		case Types_e::ARRAY:
			{
				if (indent >= 0)
					emitIndent(writer, indent);

				if (n == startNode || !n->hasName() || strcmp(n->nameCstr(), "__root__") == 0) 
				{
					emitText(writer, '[');
				}
				else
				{
					emitText(writer, '"');
					emitText(writer, n->nameCstr());
					emitText(writer, (indent >= 0) ? "\": [" : "\":[");
				}

                auto members = n->getNodes();
                for (auto it = members.begin(); it != members.end(); ++it)
                {
                    if (it != members.begin())
                        emitText(writer, ',');

					cjson::stringify_worker(
						*it, 
						writer, 
						(indent >= 0) ? indent + 1 : -1,
						startNode);
				}

				if (indent >= 0)
					emitIndent(writer, indent);

				emitText(writer, ']');
			}
			break;

		case Types_e::OBJECT:
			{
				if (indent >= 0)
					emitIndent(writer, indent);

				if (n == startNode || !n->hasName() || strcmp(n->nameCstr(), "__root__") == 0)
				{
					emitText(writer, '{');
				}
				else
				{
					emitText(writer, '"');
					emitText(writer, n->nameCstr());
					emitText(writer, (indent >= 0) ? "\": {" : "\":{");
				}

				auto members = n->getNodes();
				for (auto it = members.begin(); it != members.end(); ++it)
				{
					if (it != members.begin())
						emitText(writer, ',');

					stringify_worker(
						*it, 
						writer, 
						(indent >= 0) ? indent + 1 : -1,
						startNode);
				}

				if (indent >= 0)
					emitIndent(writer, indent);

				emitText(writer, '}');
			}

			break;
		case Types_e::VOIDED: break;
		default: break;
	}
}

// local SkipJunk - helper function for document parser. Skips spaces, line breaks, etc.
// updates cursor as a reference.
inline void SkipJunk(char* & readPtr)
{
	while (*readPtr && *readPtr <= 32) // move up if not null, but low ascii
		++readPtr;
}

// ParseNumeric - helper function to advance cursor past number
// and return number as std::string (readPtr is updated)
__inline std::string ParseNumeric(char*& readPtr, bool& isDouble)
{
    const auto readStart = readPtr;
	isDouble = false;

	// is a number, decimal and possibly negative
	while (*readPtr >= '-' && *readPtr <= '9')
	{
		if (*readPtr == '.')
			isDouble = true;
		++readPtr;
	}

    return std::string(readStart, readPtr - readStart);
}

cjson* cjson::parseBranch(cjson* n, char* & readPtr, const bool embedding)
{
	if (!*readPtr)
		return cjson::makeDocument();

    auto rootInit = false;

	if (embedding)
	{
        SkipJunk(readPtr);

		if (*readPtr == '{')
			n->setType(Types_e::OBJECT);
		else
			n->setType(Types_e::ARRAY);

		++readPtr;
		rootInit = true;
	}
		
	if (n == nullptr)
	{
		SkipJunk(readPtr);

		if (*readPtr != '[' && *readPtr != '{')
		{
			return makeDocument();
		}

		if (*readPtr == '{')
		{
			n = makeDocument();
		}
		else
		{
			n = makeDocument();
			n->setType(Types_e::ARRAY);
		}

		rootInit = true;

		++readPtr;
	}

    while (*readPtr && *readPtr != 0x1a)
	{
		// this is a number out at the main loop, so we are appending an array probably

		if (*readPtr == '}' || *readPtr == ']')
		{
			++readPtr;
			return n;
		}

		// this is a doc without a string identifier, or an document in an array likely
		if (*readPtr == '{')
		{
			++readPtr;
			if (!n->parentNode && !rootInit)
			{
				n->setType(Types_e::OBJECT);
				rootInit = true;
				//cjson::ParseBranch(N, readPtr);
			}
			else
			{
				parseBranch(n->pushObject(), readPtr);
			}
		}
		// this is an array without a string identifier, or an array in an array likely
		else if (*readPtr == '[')
		{
			++readPtr;
			if (!n->parentNode && !rootInit)
			{
				n->setType(Types_e::ARRAY);
				rootInit = true;
			}
			else
			{
				parseBranch(n->pushArray(), readPtr);
			}
		}
		else if (*readPtr == '-' || (*readPtr >= '0' && *readPtr <= '9')) // number or neg number)
		{
			auto isDouble = false;
			auto numericString = ParseNumeric(readPtr, isDouble);

			if (isDouble)
			{
				char* endp;
				n->push(strtod(numericString.c_str(), &endp));
			}
			else
			{
				char* endp;
				n->push(strtoll(numericString.c_str(), &endp, 10));
			}
		}
		else if (*readPtr == '"')
		{
			++readPtr;

			string accumulator;
			while (*readPtr != 0)
			{
				if (*readPtr == '"')
					break;
				if (*readPtr == '\\')
				{
					++readPtr;
					switch (*readPtr)
					{
						case 'r':
							accumulator += '\r';
							break;
						case 'n':
							accumulator += '\n';
							break;
						case 't':
							accumulator += '\t';
							break;
						case 'f':
							accumulator += '\f';
							break;
						case 'b':
							accumulator += '\b';
							break;
						case 'v':
							accumulator += '\v';
							break;
						case '/':
							accumulator += '/';
							break;
						case '\\':
							accumulator += '\\';
							break;
						case '"':
							accumulator += '"';
							break;
						case '\'':
							accumulator += '\'';
							break;
						default:
							accumulator += '\\';
							break;
					}
				}
				else
					accumulator += *readPtr;

				++readPtr;
			}

		    const auto name = accumulator;

			++readPtr;

			SkipJunk(readPtr);

			// if there is a comma right after a string, 
			// then we are appending an array of strings
			if (*readPtr == ',' || *readPtr == ']')
			{
				n->push(name);
			}
			else if (*readPtr == ':')
			{
				++readPtr;
				SkipJunk(readPtr);

				// we have a nested document
				if (*readPtr == '{')
				{
					++readPtr;
					parseBranch(n->setObject(name), readPtr);
					continue;
				}

			    // we have a nested array
				if (*readPtr == '[')
				{
					++readPtr;
					parseBranch(n->setArray(name), readPtr);
					continue;
				}

				if (*readPtr == '"')
				{
					++readPtr;

					accumulator.clear();
					while (*readPtr != 0)
					{
						if (*readPtr == '"')
                        {
							break;
                        }
						if (*readPtr == '\\')
						{
							++readPtr;
					        switch (*readPtr)
					        {
						        case 'r':
							        accumulator += '\r';
							        break;
						        case 'n':
							        accumulator += '\n';
							        break;
						        case 't':
							        accumulator += '\t';
							        break;
						        case 'f':
							        accumulator += '\f';
							        break;
						        case 'b':
							        accumulator += '\b';
							        break;
						        case 'v':
							        accumulator += '\v';
							        break;
						        case '/':
							        accumulator += '/';
							        break;
						        case '\\':
							        accumulator += '\\';
							        break;
						        case '"':
							        accumulator += '"';
							        break;
						        case '\'':
							        accumulator += '\'';
							        break;
						        default:
							        accumulator += '\\';
							        break;
					        }
						}
						else
                        {
							accumulator += *readPtr;
                        }

						++readPtr;
					}

					n->set(name, accumulator);
				}
				else if (*readPtr == '-' || (*readPtr >= '0' && *readPtr <= '9')) // number or neg number
				{
					auto isDouble = false;
					auto value = ParseNumeric(readPtr, isDouble);

					if (isDouble)
					{
						char* endp;
						n->set(name, strtod(value.c_str(), &endp));
					}
					else
					{
						char* endp;
						n->set(name, strtoll(value.c_str(), &endp, 10));
					}

					continue;
				}
				else if (*readPtr == 'N' || *readPtr == 'n') // skip null
				{
					n->set(name);
					readPtr += 4;
					continue;
				}
				else if (*readPtr == 'u' || *readPtr == 'U') // skip undefined
				{
					readPtr += 8;
				}
				else if (*readPtr == 't' || *readPtr == 'f')
				{
					auto tf = false;

					if (*readPtr == 't')
					{
						tf = true;
						readPtr += 3;
					}
					else
						readPtr += 4;

					n->set(name, tf);
				}
				++readPtr;
			}
		}
		else
		{
			++readPtr;
		}
	};

	return n;
}

