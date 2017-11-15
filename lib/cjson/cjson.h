#pragma once
/*

cjson - a nice object driven JSON parser and serializer. 

Features:

  - while JSON will never be as easy as JSON is in JavaScript
    cjson is one of the easiers to use I've come across.
  - uses HeapStack (https://github.com/SethHamilton/HeapStack) to
    perform block allocations. All memory including cjson node objects
	are stored within the HeapStack. This eliminates the memory fragmentation
	that is typical found in DOM implemenations. HeapStack also eleminites
	all the overhead of allocating small objects and buffers (which is huge).
  - has xpath type functionality to find nodes by document path.
  - has many helpers to get, set, append, etc. when manually working
    on nodes.
  - Fast! It can Parse and Stringify at nearly the same speed (which is fast).
    On my Core i7 I was able to parse a heavily nest 185MB JSON file in 
	3891ms. I was able to serialize it out to a non-pretified 124MB JSON
	file in 3953ms. 
  - Easily incorporated into code that must call REST endpoints or where
    you want configuration in JSON rather than CONF formats.

The MIT License (MIT)

Copyright (c) 2015 Seth A. Hamilton

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

*/
#include <vector>
#include <algorithm>
#include <functional>
#include "../heapstack/heapstack.h"

enum class cjsonType : int64_t
{
	VOIDED,
	NUL,
	OBJECT,
	ARRAY,
	INT,
	DBL,
	STR,
	BOOL
};

class cjson
{
private:

	HeapStack* mem;

	cjsonType nodeType;
	char* nodeName;

	// dataUnion uses the often ignored but always awesome
	// union feature of C++
	//
	// the union allows us to view an arbitrary pointer of type dataUnion
	// as a any of the types within in. We can reference the values within
	// the union without having to do any other fussy type casting
	union dataUnion
	{
		char asStr;
		int64_t asInt;
		double asDouble;
		bool asBool;
	};

	dataUnion* nodeData;

	// if a doc or array it will have members
	//std::vector< cjson* > nodeMembers;

public:
	// members are linked list of other nodes at this
	// document level.
	// this is how array and object values are stored
	cjson* membersHead;
	cjson* membersTail;
	int memberCount;

	// next and previous sibling in to this members node list
	cjson* siblingPrev;
	cjson* siblingNext;
	cjson* parentNode;
	cjson* rootNode;
	
	using SortFunction = std::function<bool(const cjson*, const cjson*)>;

	struct curs
	{
		cjson* original;
		cjson* current;

		explicit curs(cjson* currentNode) :
			original(currentNode),
			current(currentNode)
		{ };

		// reset cursor to it's original state (the cjson node it started on)
		void reset()
		{
			current = original;
		}

		// similar to -- operator, but won't leave
		// current NULL when you hit the beginning of the list
		// it will return false and leave current at the last
		// good cjson node
		bool next()
		{
			if (current && current->siblingNext)
			{
				current = current->siblingNext;
				return true;
			}
			return false;
		}

		// similar to ++ operator, but won't leave
		// current NULL when you hit the beginning of the list
		// it will return false and leave current at the last
		// good cjson node
		bool prev()
		{
			if (current && current->siblingPrev)
			{
				current = current->siblingPrev;
				return true;
			}
			return false;
		}

		// move up to the parent node
		bool up()
		{
			if (current && current->parentNode)
			{
				current = current->parentNode;
				return true;
			}
			return false;
		}

		// move to first child
		// returns true or false, leaves current at last good node
		bool down()
		{
			if (current && current->membersHead)
			{
				current = current->membersHead;
				return true;
			}
			return false;
		}

		// get current node (or null)
		// you can also use myiter.current to access the node you want
		cjson* operator()() const
		{
			return current;
		}

		// move to next sibling, will set current to null
		// when the end found
		curs& operator++(int)
		{
			if (current && current->siblingNext)
				current = current->siblingNext;
			else
				current = nullptr;
			return *this;
		}

		// move to previous sibling, will set current to null
		// when the end found
		curs& operator--(int)
		{
			if (current && current->siblingPrev)
				current = current->siblingPrev;
			else
				current = nullptr;
			return *this;
		}
	};

	bool selfConstructed;

	cjson();
	explicit cjson(cjsonType docType);
	explicit cjson(std::string fileName);
	cjson(std::string jsonText, size_t length);
	cjson(HeapStack* MemObj, cjson* RootObj);

	cjson(const cjson&) = delete; // can't copy - actually we could... but..
	cjson(cjson&& other); // moveable 

	~cjson();

	cjson& operator=(cjson&& source) noexcept;

	/*
	-------------------------------------------------------------------------
	node information
	-------------------------------------------------------------------------
	*/

	cjsonType type() const; // returns the node type cjsonNode:: is the enum
	std::string name() const; // returns the node name or ""
	const char* nameCstr() const; // returns pointer to node name or NULL

	void setName(const char* newName);
	void setName(std::string newName);
	void setType(cjsonType Type);

	// test for node by xpath
	bool isNode(std::string xPath) const;
	bool hasName() const;

	/*
	-------------------------------------------------------------------------
	node construction / destruction

	Use MakeDocument() to create a root document node with a memory manager.

	Methods that create new nodes will call one of the following, these 
	can be called directly.

	Nodes are created detached from the document, but are associated with 
	the doucments memory manager. If you destroy the document unattached 
	node will also become invalid.

	Note: removeNode will mark a nodes type as voided. Nodes are not
	      actually pruned fromt the document. 
	      name and data will be NULLed.
	      nodes fo cjsonType::VOIDED will not be emitted by Stringify
		  VOIDED nodes will not be eimmited by functions that return
		  std::vectors.
		  Take care to avoid VOIDED nodes when iterating.
		  
	-------------------------------------------------------------------------
	*/

	cjson* createNode() const;
	cjson* createNode(cjsonType Type, const char* Name) const;
	cjson* createNode(cjsonType Type, std::string Name) const;
	void removeNode();

	cjson* hasMembers() const;
	cjson* hasParent() const;

	// return a cursor, which can be used to navigate
	// the document (see the cursor struct above for members).
	// the cursor will start at the node you call it from
	curs cursor();

	/*
	-------------------------------------------------------------------------
	node navigation
	-------------------------------------------------------------------------
	*/

	// get child node (member) names
	std::vector<std::string> getKeys() const;

	// returns all the child nodes (members) for this node 
	std::vector<cjson*> getNodes() const;

	// get a value AT index from an array or document
	cjson* at(int index) const;

	// search this node for (immediate) child of name
	cjson* find(const char* Name) const;
	cjson* find(std::string Name) const;
	cjson* findByHashValue(size_t hashedId) const;

	// helper, append a value to an array
	/*
	-------------------------------------------------------------------------
	push funtions.

	these will push nodes with a specified value into the current
	nodes members lists (this is usually an array in this case).

	These are used by the Parse function and can also be used directly.

	result is the newly created cjson object (node).
	-------------------------------------------------------------------------
	*/
	cjson* push(int64_t Value);
	cjson* push(double Value);
	cjson* push(const char* Value);
	cjson* push(std::string Value);
	cjson* push(bool Value);
	cjson* push(cjson* Node); // push a node - returns provided node
	cjson* pushNull(); // pushes a null node
	cjson* pushArray(); // append members with an array 
	cjson* pushObject(); // append members with object/sub-document 

	/*
	-------------------------------------------------------------------------
	key/value funtions.

	upsert functionality, will adds or update an existing key value pair 
	in a doc type node 
	
	i.e. "key": "value" or "key": #value, etc.
	-------------------------------------------------------------------------
	*/
	cjson* set(const char* Key, int64_t Value);
	cjson* set(std::string Key, int64_t Value);
	cjson* set(const char* Key, int32_t Value);
	cjson* set(std::string Key, int32_t Value);
	cjson* set(const char* Key, double Value);
	cjson* set(std::string Key, double Value);
	cjson* set(const char* Key, const char* Value);
	cjson* set(std::string Key, const char* Value);
	cjson* set(std::string Key, std::string Value);
	cjson* set(const char* Key, bool Value);
	cjson* set(std::string Key, bool Value);
	cjson* set(const char* Key); // sets "key": null
	cjson* set(std::string Key); // sets "key": null

	// adds a new array or returns an existing array key value type\
	// i.e. "key": []
	cjson* setArray(const char* Key);
	cjson* setArray(std::string Key);

	// adds a new or returns an existing document key value type
	// i.e. "key": {}
	cjson* setObject(const char* Key);
	cjson* setObject(std::string Key);

	// get number of items in an array
	int size() const;
	bool empty() const;


	/*
	-------------------------------------------------------------------------
	replace value functions.

	Will overwrite the current value (and type) for an existing node
	-------------------------------------------------------------------------
	*/
	void replace(int64_t Val);
	void replace(double Val);
	void replace(const char* Val);
	void replace(std::string Val);
	void replace(bool Val);
	void replace(); // sets value to NUll;

	/* 
	-------------------------------------------------------------------------
	Path Based Search Functions

    These are for searching a document and returning the value at that node.

	documents are searched using a path:

	i.e. /node/node/arrayindex/node

	a Default value is provided, if the node is not found the Default 
	will be returned.
	
	This is really useful when using json or config data where
	a document may be missing values, but defaults are required.	

	Note: Path is node relative, for full paths use your root document
	for nodes deeper into the document use a relative path.
	-------------------------------------------------------------------------
	*/
	int64_t xPathInt(std::string Path, int64_t Default) const;
	bool xPathBool(std::string Path, bool Default) const;
	double xPathDouble(std::string Path, double Default) const;
	const char* xPathCstr(const char* Path, const char* Default) const;
	std::string xPathString(std::string Path, std::string Default) const;
	// returns a node or NULL;
	cjson* xPath(std::string Path) const;

	std::string xPath() const; // return the current xpath;

	/*
	-------------------------------------------------------------------------
	Value Access Functions

	These retrieve the value at a node.

	These functions return a true/false (success)
	return the value (if successful) as a reference. 

	These functions can be used in conditional
	
	i.e.

	int64_t someInt;
	double someDouble;

	if (someNode->isInt( someInt )) 
	   // do something
	else if (someNode->isDouble( someDouble ))
	   // do something else

	-------------------------------------------------------------------------
	*/
	bool isStringCstr(char* & Value) const;
	bool isString(std::string& Value) const;
	bool isInt(int64_t& Value) const;
	bool isDouble(double& Value) const;
	bool isBool(bool& Value) const;
	bool isNull() const;

	int64_t getInt() const;
	double getDouble() const;
	const char* getCstr() const;
	string getString() const;
	bool getBool() const;

	/*
	-------------------------------------------------------------------------
	Document import/export functions
	-------------------------------------------------------------------------
	*/
	static cjson* Parse(const char* JSON, cjson* root = nullptr, bool embedded = false);
	static cjson* Parse(std::string JSON, cjson* root = nullptr, bool embedded = false);

	// returns char* you must call delete[] on the result
	// memory is allocated with pooled memory, call releaseStringifyPtr to release
	// this buffer
	static char* StringifyCstr(const cjson* N, int64_t& length, bool pretty = false);

	static HeapStack* StringifyHeapStack(cjson* N, int64_t& length, bool pretty = false);
	// returns std::string (calls char* version internally)
	// this version is slightly slower than the char* version
	// on multi-megabyte JSON documents
	static std::string Stringify(cjson* N, bool pretty = false);

	// release memory allocated by PooledMem when flatten is called
	static void releaseStringifyPtr(char* strPtr);

	// completely free a document and all it's children
	// all nodes in the document become invalid immediately
	static void DisposeDocument(cjson* Document);
	// create a root node (with heapstack object).
	static cjson* MakeDocument();

	static cjson* fromFile(std::string fileName, cjson* root = nullptr);
	static bool toFile(std::string fileName, cjson* root, bool pretty = true);

	void sortMembers(SortFunction sortLambda)
	{
		if (membersHead == nullptr)
			return;

		std::vector<cjson*> sortVector;
		sortVector.clear();

		auto it = membersHead;
		while (it)
		{
			if (it->nodeType == cjsonType::VOIDED)
				continue;
			sortVector.push_back(it);
			it = it->siblingNext;
		}

		std::sort(sortVector.begin(), sortVector.end(), sortLambda);

		// relink the nodes in place (where they were allocated)
		membersHead = sortVector.front();
		cjson* last = nullptr;
		for (auto n:sortVector)
		{
			n->siblingPrev = last;
			if (last)
				last->siblingNext = n;
			last = n;
		}
		last->siblingNext = nullptr;
		membersTail = sortVector.back();

	}

	void recurseSort(string nodeName, SortFunction sortLambda) 
	{
		__recurseSort(nodeName, this, sortLambda);
	}

	size_t hashed() const;
	
private:

	// this recurses the tree, it would be possible to make a non-recursive 
	// version at some point.

	static void __recurseSort(string nodeName, cjson* branch, SortFunction sortLambda)
	{

		if (branch->name() == nodeName)
			branch->sortMembers(sortLambda);

		auto it = branch->membersHead;

		while (it)
		{
			if (it->nodeType == cjsonType::VOIDED)
				continue;
			__recurseSort(nodeName, it, sortLambda);
			it = it->siblingNext;
		}
	}

	// Each node can have children. These are implemented as 
	// linked list of nodes. The link properties as well as head
	// and tail properties are present in each node.
	// Link will set maintain membersHead and membersTail within
	// the node that calls link as well as maintain siblingNext
	// and siblingPrev for newNode and it's siblings.
	void Link(cjson* newNode);
	static cjson* ParseBranch(cjson* N, char* & readPtr, bool embedding = false);

	// function used by xPath functions
	cjson* GetNodeByPath(std::string Path) const;

	// worker used stringifyC
	void Stringify_worker(const cjson* N, HeapStack& writer, int indent, const cjson* startNode) const;

	// helper used to find the index of current node in a members list
	int getIndex() const;
};

namespace std
{
	template <>
	struct hash<cjson>
	{
		size_t operator()(const cjson& x) const
		{
			switch (x.type())
			{
				case cjsonType::INT: 
					return static_cast<size_t>(x.getInt());
				break;
				case cjsonType::DBL: 
					return static_cast<size_t>(x.getDouble() * 10000);
				break;
				case cjsonType::STR: 
				{
					hash<string> hasher;
					return hasher(x.getString());
				}
				break;
				case cjsonType::BOOL: 
					return x.getBool() ? 1 : 0;
				break;
				default: 
					return 0;
				break;
			}
		}
	};
};

