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
void Split(std::string Source, char Token, std::vector<std::string>& result)
{
	result.clear(); // clear it just in case it is being reused

	if (!Source.length())
		return;

	size_t length = Source.length();
	size_t start = 0;
	size_t end = Source.find(Token, 0);

	size_t diff;

	while (end != -1)
	{
		// removes empty splits 
		if (end - start == 0)
		{
			while (start != length && Source[start] == Token)
				start++;

			if ((end = Source.find(Token, start)) == -1)
				break;
		}

		diff = end - start;

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
	nodeType(cjsonType::VOIDED),
	nodeName(nullptr),
	nodeData(nullptr),
	membersHead(nullptr),
	membersTail(nullptr),
	memberCount(0),
	siblingPrev(nullptr),
	siblingNext(nullptr),
	parentNode(nullptr),
	rootNode(this),
	selfConstructed(true)
{
	setName("__root__");
	setType(cjsonType::OBJECT);
}

cjson::cjson(cjsonType docType):
	cjson()
{
	nodeType =docType;
};

cjson::cjson(std::string fileName):
	cjson()
{
	fromFile(fileName, this);
};

cjson::cjson(std::string jsonText, size_t length):
	cjson()
{
	Parse(jsonText, this);
};



cjson::cjson(HeapStack* MemObj, cjson* RootObj) :
	mem(MemObj),
	nodeType(cjsonType::VOIDED),
	nodeName(nullptr),
	nodeData(nullptr),
	membersHead(nullptr),
	membersTail(nullptr),
	memberCount(0),
	siblingPrev(nullptr),
	siblingNext(nullptr),
	parentNode(nullptr),
	rootNode(RootObj),
	selfConstructed(false)
{}

cjson::cjson(cjson&& other) :
	mem(other.mem),
	nodeType(other.nodeType),
	nodeName(other.nodeName),
	nodeData(other.nodeData),
	membersHead(other.membersHead),
	membersTail(other.membersTail),
	memberCount(other.memberCount),
	siblingPrev(other.siblingPrev),
	siblingNext(other.siblingNext),
	parentNode(other.parentNode),
	rootNode(other.rootNode),
	selfConstructed(other.selfConstructed)
{
	other.selfConstructed = false;
	other.mem = nullptr;
	other.membersHead = nullptr;
	other.membersTail = nullptr;
	other.siblingNext = nullptr;
	other.siblingPrev = nullptr;
	other.parentNode = nullptr;
	other.rootNode = nullptr;
	other.nodeType = cjsonType::VOIDED;
};

cjson::~cjson()
{
	// this destructor does nothing and is never
	// called because we are using "placement new" for
	// our allocations
	if (selfConstructed)
		delete mem;
};

cjsonType cjson::type() const
{
	return nodeType;
};

void cjson::setName(const char* newName)
{
	if (newName)
	{
		size_t len = strlen(newName) + 1;
		this->nodeName = mem->newPtr(len);
		memcpy(this->nodeName, newName, len);
	}
};

void cjson::setName(std::string newName)
{
	setName(newName.c_str());
};

void cjson::setType(cjsonType Type)
{
	nodeType = Type;
}

cjson* cjson::createNode() const
{
	// using placement new to put object in HeapStack
	auto nodePtr = mem->newPtr(sizeof(cjson));
	return new(nodePtr) cjson(mem, rootNode);
};

cjson* cjson::createNode(cjsonType Type, const char* Name) const
{
	cjson* newNode = createNode();
	newNode->setName(Name);
	newNode->nodeType = Type;
	return newNode;
};

cjson* cjson::createNode(cjsonType Type, std::string Name) const
{
	cjson* newNode = createNode();
	newNode->setName(Name.c_str());
	newNode->nodeType = Type;
	return newNode;
};

void cjson::removeNode()
{
	nodeType = cjsonType::VOIDED;
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
	curs newIter(this);
	return newIter;
}

bool cjson::isNode(std::string xPath) const
{
	return (GetNodeByPath(xPath)) ? true : false;
};

bool cjson::hasName() const
{
	return (nodeName && *static_cast<char*>(nodeName) != 0) ? true : false;
};

std::vector<std::string> cjson::getKeys() const
{
	// allocate a list of the required size for all the node names
	int count = this->size();
	std::vector<std::string> names;
	names.reserve(count);

	auto n = membersHead;

	while (n)
	{
		if (n->nodeName && n->nodeType != cjsonType::VOIDED)
			names.push_back(std::string(n->nodeName));
		n = n->siblingNext;
	}

	return names;
}

std::vector<cjson*> cjson::getNodes() const
{
	int count = this->size();
	std::vector<cjson*> array;
	array.reserve(count);

	// only return for ARRAY and OBJECT type nodes
	if (nodeType != cjsonType::ARRAY &&
		nodeType != cjsonType::OBJECT)
		return array; // return an empty array

	auto n = membersHead;

	while (n)
	{
		if (n->nodeType != cjsonType::VOIDED)
			array.push_back(n);
		n = n->siblingNext;
	}

	return array;
}

cjson* cjson::at(int index) const
{
	// linked lists don't really do random access
	// so this won't be the fasted function ever written
	//
	// if repeated iteration is needed using one of the 
	// array type functions would probably be best
	//
	// i.e. GetNodes();

	int iter = 0;
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

cjson* cjson::find(const char* Name) const
{
	auto n = membersHead;

	while (n)
	{
		if (n->nodeName && strcmp(n->nodeName, Name) == 0)
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

	while (1) // rewind
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

cjson* cjson::find(std::string Name) const
{
	return find(Name.c_str());
}

cjson* cjson::findByHashValue(size_t hashedId) const
{
	auto n = membersHead;

	hash<cjson> hasher;

	while (n)
	{
		auto hashId = hasher(*n);
		if (hashId == hashedId)
			return n;

		n = n->siblingNext;
	}
	return nullptr;
}

// helper, append a value to an array
cjson* cjson::push(int64_t Value)
{
	cjson* newNode = createNode();
	// initialize the node
	newNode->replace(Value);
	Link(newNode);
	return newNode;
}

// helper, append a value to an array
cjson* cjson::push(double Value)
{
	cjson* newNode = createNode();
	// initialize the node
	newNode->replace(Value);
	Link(newNode);
	return newNode;
};

// helper, append a value to an array
cjson* cjson::push(const char* Value)
{
	cjson* newNode = createNode();
	// initialize the node
	newNode->replace(Value);
	Link(newNode);
	return newNode;
};

// helper, append a value to an array
cjson* cjson::push(std::string Value)
{
	cjson* newNode = createNode();
	// initialize the node
	newNode->replace(Value);
	Link(newNode);
	return newNode;
};

// helper, append a value to an array
cjson* cjson::push(bool Value)
{
	cjson* newNode = createNode();
	// initialize the node
	newNode->replace(Value);
	Link(newNode);
	return newNode;
};

// helper, append a value to an array
cjson* cjson::push(cjson* Node)
{
	Link(Node);
	return Node;
};

// append a nested array onto the array
cjson* cjson::pushArray()
{
	cjson* newNode = createNode();
	// initialize the node
	newNode->nodeType = cjsonType::ARRAY;

	Link(newNode);
	return newNode;
};

// append a nested array onto the array
cjson* cjson::pushNull()
{
	cjson* newNode = createNode();
	// initialize the node
	newNode->nodeType = cjsonType::NUL;
	Link(newNode);
	return newNode;
};

// append a nested document onto the array
cjson* cjson::pushObject()
{
	cjson* newNode = createNode();

	// initialize the node
	newNode->nodeType = cjsonType::OBJECT;

	Link(newNode);
	return newNode;
};

// adds or updates an existing key value pair in a doc type node
cjson* cjson::set(const char* Key, int64_t Value)
{
	cjson* Node = find(Key);

	if (!Node)
	{
		cjson* newNode = createNode();

		newNode->setName(Key);
		Link(newNode);

		Node = newNode;
	}

	Node->replace(Value);
	return Node;
};

cjson* cjson::set(std::string Key, int64_t Value)
{
	return set(Key.c_str(), Value);
}

cjson* cjson::set(const char* Key, int32_t Value)
{
	return set(Key, static_cast<int64_t>(Value));
};

cjson* cjson::set(std::string Key, int32_t Value)
{
	return set(Key, static_cast<int64_t>(Value));
}

// adds or updates an existing key value pair in a doc type node
cjson* cjson::set(const char* Key, double Value)
{
	cjson* Node = find(Key);

	if (!Node)
	{
		cjson* newNode = createNode();

		newNode->setName(Key);

		Link(newNode);

		Node = newNode;
	}

	Node->replace(Value);
	return Node;
};

cjson* cjson::set(std::string Key, double Value)
{
	return set(Key.c_str(), Value);
}

// adds or updates an existing key value pair in a doc type node
cjson* cjson::set(const char* Key, const char* Value)
{
	cjson* Node = find(Key);

	if (!Node)
	{
		cjson* newNode = createNode();

		newNode->setName(Key);
		Link(newNode);

		Node = newNode;
	}

	Node->replace(Value);
	return Node;
}

cjson* cjson::set(std::string Key, const char* Value)
{
	return set(Key.c_str(), Value);
};

cjson* cjson::set(std::string Key, std::string Value)
{
	return set(Key.c_str(), Value.c_str());
};

cjson* cjson::set(const char* Key, bool Value)
{
	cjson* Node = find(Key);

	if (!Node)
	{
		cjson* newNode = createNode();

		newNode->setName(Key);
		Link(newNode);

		Node = newNode;
	}

	Node->replace(Value);
	return Node;
};

cjson* cjson::set(std::string Key, bool Value)
{
	return set(Key.c_str(), Value);
};

// adds null
cjson* cjson::set(const char* Key)
{
	cjson* Node = find(Key);

	if (!Node)
	{
		cjson* newNode = createNode();

		newNode->setName(Key);
		Link(newNode);
		Node = newNode;
	}

	Node->nodeType = cjsonType::NUL;
	return Node;
}

cjson* cjson::set(std::string Key)
{
	return set(Key.c_str());
}

// adds or updates an existing key value pair in a doc type node

// adds a new array or returns an existing array key value type
cjson* cjson::setArray(const char* Key)
{
	cjson* Node = find(Key);

	if (!Node)
	{
		cjson* newNode = createNode();

		newNode->setName(Key);
		newNode->nodeType = cjsonType::ARRAY;
		Link(newNode);

		return newNode;
	}

	return Node;
};

cjson* cjson::setArray(std::string Key)
{
	return setArray(Key.c_str());
};

// adds a new or returns an existing document key value type
cjson* cjson::setObject(const char* Key)
{
	auto Node = find(Key);

	if (!Node)
	{
		auto newNode = createNode();

		newNode->setName(Key);
		newNode->nodeType = cjsonType::OBJECT;
		Link(newNode);

		return newNode;
	}

	return Node;
}

cjson* cjson::setObject(std::string Key)
{
	return setObject(Key.c_str());
}

// get number of items in an array
int cjson::size() const
{
	return memberCount;
}

bool cjson::empty() const
{
	return (memberCount == 0);
}

void cjson::replace(int64_t Val)
{
	nodeType = cjsonType::INT;
	nodeData = reinterpret_cast<dataUnion*>(mem->newPtr(sizeof(Val)));
	nodeData->asInt = Val;
}

void cjson::replace(double Val)
{
	nodeType = cjsonType::DBL;
	nodeData = reinterpret_cast<dataUnion*>(mem->newPtr(sizeof(Val)));
	nodeData->asDouble = Val;
}

void cjson::replace(const char* Val)
{
	nodeType = cjsonType::STR;

	size_t len = strlen(Val) + 1;
	char* textPtr = mem->newPtr(len);
	// we are going to copy the string to textPtr
	// but we have to point nodeData to this as well
	nodeData = reinterpret_cast<dataUnion*>(textPtr);
	memcpy(textPtr, Val, len);
}

void cjson::replace(std::string Val)
{
	replace(Val.c_str());
}

void cjson::replace()
{
	nodeType = cjsonType::NUL;
	nodeData = nullptr;
}

void cjson::replace(bool Val)
{
	nodeType = cjsonType::BOOL;
	nodeData = reinterpret_cast<dataUnion*>(mem->newPtr(sizeof(Val)));
	nodeData->asBool = Val;
}

int64_t cjson::xPathInt(std::string Path, int64_t Default) const
{
	cjson* N = GetNodeByPath(Path);
	if (N && N->nodeType == cjsonType::INT)
		return N->nodeData->asInt;
	return Default;
}

bool cjson::xPathBool(std::string Path, bool Default) const
{
	cjson* N = GetNodeByPath(Path);
	if (N && N->nodeType == cjsonType::BOOL)
		return N->nodeData->asBool;
	return Default;
}

double cjson::xPathDouble(std::string Path, double Default) const
{
	cjson* N = GetNodeByPath(Path);
	if (N && N->nodeType == cjsonType::DBL)
		return N->nodeData->asDouble;
	return Default;
}

const char* cjson::xPathCstr(const char* Path, const char* Default) const
{
	cjson* N = GetNodeByPath(Path);
	if (N && N->nodeType == cjsonType::STR)
		return &N->nodeData->asStr;
	return Default;
}

std::string cjson::xPathString(std::string Path, std::string Default) const
{
	const char* res = xPathCstr(const_cast<char*>(Path.c_str()), Default.c_str());
	return std::string(res);
}

cjson* cjson::xPath(std::string Path) const
{
	cjson* N = GetNodeByPath(Path);
	if (N)
		return N;
	return nullptr;
};

std::string cjson::xPath() const
{
	std::string path;

	const cjson* n = this;

	while (n)
	{
		if (n != this)
			path = '/' + path;

		const char * name = n->nameCstr();

		if (name && *name) // is there a name?
		{
			if (strcmp(name, "__root__") == 0)
				break;

			path = name + path;
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

bool cjson::isStringCstr(char* & Value) const
{
	if (nodeType == cjsonType::STR)
	{
		Value = reinterpret_cast<char*>(nodeData);
		return true;
	}
	return false;
};

bool cjson::isString(std::string& Value) const
{
	if (nodeType == cjsonType::STR)
	{
		Value = reinterpret_cast<char*>(nodeData);
		return true;
	}
	return false;
};

bool cjson::isInt(int64_t& Value) const
{
	if (nodeType == cjsonType::INT)
	{
		Value = nodeData->asInt;
		return true;
	}
	return false;
};

bool cjson::isDouble(double& Value) const
{
	if (nodeType == cjsonType::DBL)
	{
		Value = nodeData->asDouble;
		return true;
	}
	return false;
};

bool cjson::isBool(bool& Value) const
{
	if (nodeType == cjsonType::BOOL)
	{
		Value = nodeData->asBool;
		return true;
	}
	return false;
};

bool cjson::isNull() const
{
	if (nodeType == cjsonType::NUL)
		return true;
	return false;
}

int64_t cjson::getInt() const
{
	return nodeData->asInt;
}

double cjson::getDouble() const
{
	return nodeData->asDouble;
}

const char* cjson::getCstr() const
{
	return reinterpret_cast<const char*>(nodeData);
}

string cjson::getString() const
{
	return std::string(reinterpret_cast<const char*>(nodeData));
}

bool cjson::getBool() const
{
	return nodeData->asBool;
};

cjson* cjson::Parse(const char* JSON, cjson* root, bool embedded)
{
	auto cursor = const_cast<char*>(JSON);
	return cjson::ParseBranch(root, cursor, embedded);
}

cjson* cjson::Parse(std::string JSON, cjson* root, bool embedded)
{
	return cjson::Parse(JSON.c_str(), root, embedded);
};

char* cjson::StringifyCstr(const cjson* N, int64_t& length, bool pretty)
{
	HeapStack mem;	
	N->Stringify_worker(N, mem, (pretty) ? 0 : -1, N);
	
	auto end = mem.newPtr(1);
	*end = 0; // stick a null at the end of that string

	length = mem.getBytes()-1;
	return mem.flatten();
}

HeapStack* cjson::StringifyHeapStack(cjson* N, int64_t& length, bool pretty)
{
	auto mem = new HeapStack();

	N->Stringify_worker(N, *mem, (pretty) ? 0 : -1, N);

	// no null at the end

	return mem;
}


std::string cjson::Stringify(cjson* N, bool pretty)
{
	HeapStack mem;

	N->Stringify_worker(N, mem, (pretty) ? 0 : -1, N);
	auto end = mem.newPtr(1);
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

void cjson::DisposeDocument(cjson* Document)
{
	delete Document->mem;
};

cjson* cjson::MakeDocument()
{
	auto mem = new HeapStack();
	void* Data = mem->newPtr(sizeof(cjson));

	// we are going to allocate this node using "placement new"
	// in the HeapStack
	auto newNode = new(Data) cjson(mem, nullptr);

	newNode->setName("__root__");
	newNode->setType(cjsonType::OBJECT);

	return newNode;
};

cjson* cjson::fromFile(std::string fileName, cjson* root)
{
	int64_t size = openset::IO::File::FileSize((char*)fileName.c_str());

	FILE* file = fopen(fileName.c_str(), "rb");

	if (file == nullptr)
		return nullptr;

	auto data = new char[size + 1];

	fread(data, 1, size, file);
	fclose(file);

	data[size] = 0;

	if (!root)
	{
		auto doc = cjson::Parse(data);
		delete[]data;
		return doc;
	}
	else
	{
		ParseBranch(root, data);
		return root;
	}
}


bool cjson::toFile(std::string fileName, cjson* root, bool pretty)
{
	auto jText = cjson::Stringify(root, pretty);

	auto file = fopen(fileName.c_str(), "wb");

	if (file == nullptr)
		return false;

	fwrite(jText.c_str(), 1, jText.length(), file);

	fclose(file);
	return true;
}


cjson* cjson::GetNodeByPath(std::string Path) const 
{
	auto N = this;

	std::vector<std::string> parts;
	Split(Path, '/', parts);

	for (auto i = 0; i < parts.size(); i++)
	{
		switch (N->nodeType)
		{
			case cjsonType::OBJECT:
				{
					auto Next = N->find(parts[i]);

					if (!Next)
						return nullptr;

					N = Next;
				}
				break;
			case cjsonType::ARRAY:
				{
					int Index = atoi(parts[i].c_str());

					if (Index < 0 || Index >= N->size())
						return nullptr;

					N = N->at(Index);

					if (!N)
						return nullptr;
				}
				break;
			case cjsonType::VOIDED: break;
			case cjsonType::NUL: break;
			case cjsonType::INT: break;
			case cjsonType::DBL: break;
			case cjsonType::STR: break;
			case cjsonType::BOOL: break;
			default: break;
		}
	}

	return const_cast<cjson*>(N);
}

size_t cjson::hashed() const
{
	hash<cjson> hasher;
	return hasher(*this);
}

// internal add member
void cjson::Link(cjson* newNode)
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
		auto lastTail = membersTail;
		// set the current tails sibling to the node		
		membersTail->siblingNext = newNode;
		// new nodes previous is the last tail
		newNode->siblingPrev = lastTail;
		// set the current tail to the node;
		membersTail = newNode;
	}

	memberCount++;
};

void emitIndent(HeapStack& writer, int indent)
{
	if (writer.getBytes())
	{
		auto output = writer.newPtr(1);
		*output = '\n';
	}

	auto len = indent * 4; // 4 space pretty print (matches jsonlint.com)

	auto output = writer.newPtr(len);
	memset(output, ' ', len);
}

// helper for Stringify_worder
void emitText(HeapStack& writer, const char* text)
{
	auto len = strlen(text);
	auto output = writer.newPtr(len);
	memcpy(output, text, len);
	/*
		while (*text)
		{
			*writer = *text;
			++writer;
			++text;
		}
	*/
}

// helper for Stringify_worder
void emitText(HeapStack& writer, const char text)
{
	auto output = writer.newPtr(1);
	*output = text;
}

void cjson::Stringify_worker(const cjson* N, HeapStack& writer, int indent, const cjson* startNode) const
{
	std::string pad = "";
	std::string padshort = "";

	switch (N->nodeType)
	{
		case cjsonType::NUL:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (N->hasName())
			{
				emitText(writer, '"');
				emitText(writer, N->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			emitText(writer, "null");

			break;
		case cjsonType::INT:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (N->hasName())
			{
				emitText(writer, '"');
				emitText(writer, N->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			{
				char buffer[64];
				snprintf(buffer, 32, "%lld", N->nodeData->asInt);
				emitText(writer, buffer);
				//emitText(writer, std::to_string(N->nodeData->asInt).c_str());
			}

			break;
		case cjsonType::DBL:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (N->hasName())
			{
				emitText(writer, '"');
				emitText(writer, N->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			if (N->nodeData->asDouble == 0)
			{
				emitText(writer, "0.0");
			}
			else
			{
				char buffer[64];
				snprintf(buffer, 32, "%0.7f", N->nodeData->asDouble);
				emitText(writer, buffer);
				//emitText(writer, std::to_string(N->nodeData->asDouble).c_str());
			}

			break;
		case cjsonType::STR:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (N->hasName())
			{
				emitText(writer, '"');
				emitText(writer, N->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			emitText(writer, '"');

			// we are going to emit any UTF-8 or other bytes as such
			{
				auto ch = reinterpret_cast<char*>(N->nodeData);
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
		case cjsonType::BOOL:

			if (indent >= 0)
				emitIndent(writer, indent);

			if (N->hasName())
			{
				emitText(writer, '"');
				emitText(writer, N->nameCstr());
				emitText(writer, (indent >= 0) ? "\": " : "\":");
			}

			emitText(writer, (N->nodeData->asBool) ? "true" : "false");

			break;
		case cjsonType::ARRAY:
			{
				auto Count = N->size();

				if (indent >= 0)
					emitIndent(writer, indent);

				if (N == startNode || !N->hasName() || strcmp(N->nameCstr(), "__root__") == 0) 
				{
					emitText(writer, '[');
				}
				else
				{
					emitText(writer, '"');
					emitText(writer, N->nameCstr());
					emitText(writer, (indent >= 0) ? "\": [" : "\":[");
				}

				auto members = N->getNodes();
				for (auto i = 0; i < Count; i++)
				{
					if (i)
						emitText(writer, ',');

					cjson::Stringify_worker(
						members[i], 
						writer, 
						(indent >= 0) ? indent + 1 : -1,
						startNode);
				}

				if (indent >= 0)
					emitIndent(writer, indent);

				emitText(writer, ']');
			}
			break;

		case cjsonType::OBJECT:
			{
				auto Count = N->size();

				if (indent >= 0)
					emitIndent(writer, indent);

				if (N == startNode || !N->hasName() || strcmp(N->nameCstr(), "__root__") == 0)
				{
					emitText(writer, '{');
				}
				else
				{
					emitText(writer, '"');
					emitText(writer, N->nameCstr());
					emitText(writer, (indent >= 0) ? "\": {" : "\":{");
				}

				auto members = N->getNodes();

				for (auto i = 0; i < Count; i++)
				{
					if (i)
						emitText(writer, ',');

					cjson::Stringify_worker(
						members[i], 
						writer, 
						(indent >= 0) ? indent + 1 : -1,
						startNode);
				}

				if (indent >= 0)
					emitIndent(writer, indent);

				emitText(writer, '}');
			}

			break;
		case cjsonType::VOIDED: break;
		default: break;
	}
}

// SkipJunk - helper function for document parser. Skips spaces, line breaks, etc.
// updates cursor as a reference.
void SkipJunk(char* & readPtr)
{
	while (*readPtr && *readPtr <= 32) // move up if not null, but low ascii
		++readPtr;
}

// ParseNumeric - helper function to advance cursor past number
// and return number as std::string (readPtr is updated)
__inline std::string ParseNumeric(char*& readPtr, bool& isDouble)
{
	auto readStart = readPtr;
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

cjson* cjson::ParseBranch(cjson* N, char* & readPtr, bool embedding)
{
	if (!*readPtr)
		return cjson::MakeDocument();

	bool rootInit = false;

	if (embedding)
	{
		++readPtr;
		rootInit = true;
	}
		
	if (N == nullptr)
	{
		SkipJunk(readPtr);

		if (*readPtr != '[' && *readPtr != '{')
		{
			return cjson::MakeDocument();
		}

		if (*readPtr == '{')
		{
			N = cjson::MakeDocument();
		}
		else
		{
			N = cjson::MakeDocument();
			N->setType(cjsonType::ARRAY);
		}

		rootInit = true;

		++readPtr;
	}

	std::string Name;

	while (*readPtr && *readPtr != 0x1a)
	{
		// this is a number out at the main loop, so we are appending an array probably

		if (*readPtr == '}' || *readPtr == ']')
		{
			++readPtr;
			return N;
		}

		// this is a doc without a string identifier, or an document in an array likely
		if (*readPtr == '{')
		{
			++readPtr;
			if (!N->parentNode && !rootInit)
			{
				N->setType(cjsonType::OBJECT);
				rootInit = true;
				//cjson::ParseBranch(N, readPtr);
				continue;
			}
			else
			{
				auto A = N->pushObject();
				cjson::ParseBranch(A, readPtr);
			}
		}
		// this is an array without a string identifier, or an array in an array likely
		else if (*readPtr == '[')
		{
			++readPtr;
			if (!N->parentNode && !rootInit)
			{
				N->setType(cjsonType::ARRAY);
				rootInit = true;
				//cjson::ParseBranch(N, readPtr);
				continue;
			}
			else
			{
				auto A = N->pushArray();
				cjson::ParseBranch(A, readPtr);
			}
		}
		else if (*readPtr == '-' || (*readPtr >= '0' && *readPtr <= '9')) // number or neg number)
		{
			auto isDouble = false;
			auto Value = ParseNumeric(readPtr, isDouble);

			if (isDouble)
			{
				char* endp;
				auto D = strtod(Value.c_str(), &endp);
				N->push(D);
			}
			else
			{
				char* endp;
				int64_t LL = strtoll(Value.c_str(), &endp, 10);
				N->push(LL);
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
							accumulator += '\\' + *readPtr;
							break;
					}
				}
				else
					accumulator += *readPtr;

				++readPtr;
			}


			Name = accumulator;

			++readPtr;

			SkipJunk(readPtr);

			// if there is a comma right after a string, 
			// then we are appending an array of strings
			if (*readPtr == ',' || *readPtr == ']')
			{
				N->push(Name);
			}
			else if (*readPtr == ':')
			{
				++readPtr;
				SkipJunk(readPtr);

				// we have a nested document
				if (*readPtr == '{')
				{
					++readPtr;

					auto D = N->setObject(Name);
					cjson::ParseBranch(D, readPtr);

					continue;
				}
				else
				// we have a nested array
				if (*readPtr == '[')
				{
					++readPtr;

					auto A = N->setArray(Name);
					cjson::ParseBranch(A, readPtr);

					continue;
				}
				else if (*readPtr == '"')
				{
					++readPtr;

					accumulator.clear();
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
								case '"':
									accumulator += '"';
									break;
								default:
									accumulator += '\\' + *readPtr;
									break;

							}
						}
						else
							accumulator += *readPtr;

						++readPtr;
					}

					N->set(Name, accumulator);
				}
				else if (*readPtr == '-' || (*readPtr >= '0' && *readPtr <= '9')) // number or neg number
				{
					auto isDouble = false;
					auto Value = ParseNumeric(readPtr, isDouble);

					if (isDouble)
					{
						char* endp;
						auto D = strtod(Value.c_str(), &endp);
						N->set(Name, D);
					}
					else
					{
						char* endp;
						int64_t LL = strtoll(Value.c_str(), &endp, 10);
						N->set(Name, LL);
					}

					continue;
				}
				else if (*readPtr == 'N' || *readPtr == 'n') // skip null
				{
					N->set(Name);
					readPtr += 4;
					continue;
				}
				else if (*readPtr == 'u' || *readPtr == 'U') // skip undefined
				{
					readPtr += 8;
				}
				else if (*readPtr == 't' || *readPtr == 'f')
				{
					auto TF = false;

					if (*readPtr == 't')
					{
						TF = true;
						readPtr += 3;
					}
					else
						readPtr += 4;

					N->set(Name, TF);
				}
				++readPtr;
			}
		}
		else
		{
			++readPtr;
		}
	};

	return N;
}

