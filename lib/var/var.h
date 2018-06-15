#pragma once // if you compiler doesn't understand this it won't compile this

/*
cvar - a cvariant type for C++ that works much like a 'cvar' in JavaScript

version - 1.1

Features:

- assignment operator overloads so you can assign using =
- cast operator overloads allow you to get a value without ()
  i.e. int x = mycvar;
- provides conversion between all internal types (bool to string, float to int,
  string to double, int to bool, and so on)
- ostream << overload
- most of the == != > >= < <= + += - -= / /= * *= cvariations for all types
  implemented (with support for POD types of int32, int64, bool, float, double
  and string this became a lot of boilerplate code, but it's fast).

conversion to strings:

bool will convert to the text true/false
numbers will convert to their textual values.

conversion from strings:

when converting a string to bool a text value of "false", "0" or 0 length will
return false, anything else will return true;

when converting to numbers parsing will stop when no more numeric data is in
the string (i.e. "1234.56 test" will convert to 1234.56).

when converting floats/doubles to integers the numbers are truncated much like when
using a cast.

note: if you see warnings about ambiguity add a cast, i.e. if calling a
	  function with multiple overloads use (int)mycvar or (double)mycvar or
	  (std::string)mycvar etc.

note: uses std::string in the back-end, which works, but ideally we would
	  use a static buffer, or a heap buffer (or one that switches to a heap
	  if the static was to small). I like std::string but it makes small
	  allocations, which depending on your workload may not be ideal.

JavaScript like functions for converting a type

User Defined Literal:
	append _cvar to any constant needed to ensure it is being treated as cvar.
	This isn't often necessary but can be when types are ambiguous or when dealing
	with strings: i.e.
		cvar somecvar = "1234" + 5; // this will actually compile to
								  // 5 characters into string "1234" (whoops)

		cvar somecvar = "1234"_cvar + 5; will compile




The MIT License (MIT)

Copyright (c) 2015 Seth A. Hamilton

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sub-license, and/or sell
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

#include <iomanip>
#include <string>
#include <cstdlib>
#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <cstring>
#include <climits>

class cvar;

namespace std
{
	template <>
	struct hash<cvar>
	{
		size_t operator()(const cvar& v) const;
	};
}

using namespace std;

class cvar
{	
public:

	enum class valueType : int
	{
		INT32,
		INT64,
		FLT,
		DBL,
		STR,
		BOOL,
		LIST,
		DICT,
		SET,
		REF
	};

	using List = std::vector<cvar>;
	using Dict = std::unordered_map<cvar, cvar>;
	using Set = std::unordered_set<cvar>;

private:

	union dataUnion
	{
		int64_t asInt64;
		int32_t asInt32;
		double asDouble;
		float asFloat;
		bool asBool;
	};

	valueType type;
	dataUnion value = {0};
	dataUnion lastValue = {0}; // used as temporary buffer during conversion so we don't overwrite originals

	std::string valueString;

	mutable List* listValue = nullptr;
	mutable Dict* dictValue = nullptr;
	mutable Set* setValue = nullptr;

	cvar* reference = nullptr;

public:

	using l = std::vector<cvar>;
	using o = std::pair<cvar, cvar>;

	cvar() :
		type(valueType::INT64) // default type 
	{
		value.asInt64 = 0; // sets all values to 0
	}

	cvar(cvar&& source) noexcept
	{
		this->type = source.type;
		this->value = source.value;
		this->valueString = std::move(source.valueString);

		if (source.dictValue)
		{
			this->dictValue = source.dictValue;
			source.dictValue = nullptr;
		}

		if (source.setValue)
		{
			this->setValue = source.setValue;
			source.setValue = nullptr;
		}

		if (source.listValue)
		{
			this->listValue = source.listValue;
			source.listValue = nullptr;
		}

		source.type = valueType::INT64;
		source.value.asInt64 = 0;
	}

	cvar(const cvar& source)
	{
		this->type = source.type;
		this->value = source.value;
		this->valueString = source.valueString;

		if (source.dictValue && this->type == valueType::DICT)
		{
			this->dict();
			*dictValue = *source.dictValue;
		}

		if (source.setValue && this->type == valueType::SET)
		{
			this->set();
			*setValue = *source.setValue;
		}

		if (source.listValue && this->type == valueType::LIST)
		{
			this->list();
			*listValue = *source.listValue;
		}
	}

	// assignment constructors 
	// so you can go:
	// cvar mycvar = 5;

	cvar(const valueType type):
		type(type)
	{
		switch (type)
		{
			case valueType::LIST: 
				list();
			break;
			case valueType::DICT: 
				dict();
			break;
			case valueType::SET: 
				set();
			break;
			default: ;
		}
	}

	cvar(const int val) :
		type(valueType::INT32)
	{
		value.asInt32 = val;
	}

	cvar(const int64_t val) :
		type(valueType::INT64)
	{
		value.asInt64 = val;
	}

#ifndef _MSC_VER
    cvar(const long long int val) :
        type(valueType::INT64)
    {
        value.asInt64 = static_cast<int64_t>(val);
    }
#endif

	cvar(const float val) :
		type(valueType::FLT)
	{
		value.asFloat = val;
	}

	cvar(const double val) :
		type(valueType::DBL)
	{
		value.asDouble = val;
	}

	cvar(const char* val) :
		type(valueType::STR)
	{
		valueString.assign(val);
	}

	cvar(const std::string& val) :
		type(valueType::STR)
	{
		value.asInt64 = -1;
		valueString = val;
	}

	cvar(const bool val) :
		type(valueType::BOOL)
	{
		value.asBool = val;
	}
			
	cvar(const std::vector<cvar> &val) :
		type(valueType::LIST)
	{
		listValue = new List();
		*listValue = val;
	}

	cvar(cvar* reference) :
		type(valueType::REF),
		reference(reference)
	{
	}
		
	template<typename K, typename V>
	cvar(const std::pair<K, V>& source)
	{
		dict();
		(*dictValue)[source.first] = source.second;
	}

	//cvar(const std::initializer_list<cvar>& source);

	cvar(const std::initializer_list<std::pair<cvar,cvar>> &source) : cvar()
	{
		dict();
		for (const auto &item : source)
			(*dictValue)[cvar{ item.first }] = cvar{ item.second };
	}

	~cvar()
	{
        clear();
	}		

    void clear()
	{
        if (listValue)
        {
            delete listValue;
            listValue = nullptr;
        }
        if (dictValue)
        {
            delete dictValue;
            dictValue = nullptr;
        }
        if (setValue)
        {
            delete setValue;
            setValue = nullptr;
        }
        type = valueType::INT64;
        value.asInt64 = 0;
        reference = nullptr;
	}

	// makes a variable have a None value (We use LLONG_MIN, because it's improbable to occur in a script)
	void none()
	{
		*this = static_cast<int64_t>(LLONG_MIN);
	}

	bool isNone() const
	{
		return *this == static_cast<int64_t>(LLONG_MIN);
	}

	// does this evaulate to false?
	bool isEvalFalse() const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			return this->getInt64() == 0;
		case valueType::BOOL:
			return this->getBool() == false;
		case valueType::FLT:
		case valueType::DBL:
			return this->getDouble() == 0;
		case valueType::STR:
			return this->valueString.length() == 0;
		case valueType::LIST:
			return this->listValue->size() == 0;
		case valueType::DICT:
			return this->dictValue->size() == 0;
		case valueType::SET:
			return this->setValue->size() == 0;
		default:
			return true;
		}
	}

	bool isEvalTrue() const
	{
		return !isEvalFalse();
	}

	// turns cvar into empty Dict like python/js: some_dict = {} or some_dict = dict()
	void dict()
	{
		if (listValue) // do any needed cleanup
		{
			delete listValue;
			listValue = nullptr;
		}
		if (setValue)
		{
			delete setValue;
			setValue = nullptr;
		}

		type = valueType::DICT;
		if (!dictValue)
			dictValue = new Dict();
		else
			dictValue->clear();
	}

	// turns cvar into empty Dict like python/js: some_dict = {} or some_dict = dict()
	void set()
	{
		if (listValue) // do any needed cleanup
		{
			delete listValue;
			listValue = nullptr;
		}
		if (dictValue) // do any needed cleanup
		{
			delete dictValue;
			dictValue = nullptr;
		}

		type = valueType::SET;
		if (!setValue)
			setValue = new Set();
		else
			setValue->clear();
	}

	// turns cvar into empty List like python/js: some_list = [] or some_list = list()
	void list()
	{
		if (dictValue) // do any needed cleanup
		{
			delete dictValue;
			dictValue = nullptr;
		}
		if (setValue)
		{
			delete setValue;
			setValue = nullptr;
		}

		type = valueType::LIST;
		if (!listValue)
			listValue = new List();
		else
			listValue->clear();
	}

	Dict* getDict() const
	{
		if (type != valueType::DICT)
			throw std::runtime_error("not a dictionary");

		if (!dictValue)
			dictValue = new Dict();

		return dictValue;
	}

	List* getList() const
	{
		if (type != valueType::LIST)
			throw std::runtime_error("not a list");

		if (!listValue)
			listValue = new List();

		return listValue;
	}

	Set* getSet() const
	{
		if (type != valueType::SET)
			throw std::runtime_error("not a set");

		if (!setValue)
			setValue = new Set();

		return setValue;
	}

	bool contains(const cvar& key) const
	{
		if (type == valueType::LIST)
			//return (listValue && key.getInt32() < listValue->size()) ? true : false;
		{
			for (auto&& item : *listValue)
				if (item == key)
					return true;
			return false;
		}
		if (type == valueType::DICT)
			return dictValue ? dictValue->count(key) : false;
		if (type == valueType::SET)
			return setValue ? setValue->count(key) : false;

		throw std::runtime_error("not a dictionary/list/set");
	}

	cvar* getMemberPtr(cvar& key) const
	{
		if (type == valueType::LIST)
			return (listValue && key.getInt32() < static_cast<int>(listValue->size())) ? &(*listValue)[key.getInt32()] : nullptr;
		if (type == valueType::DICT)
			return dictValue ? &(*dictValue)[key] : nullptr;
		return nullptr;
	}

	valueType typeof() const
	{
		return type;
	}

private:

	static std::string trimZeros(std::string number)
	{
		while (number.length() > 2 && number[number.length() - 1] == '0')
			number.erase(number.length() - 1);
		if (number[number.length() - 1] == '.')
			number += "0";
		return number;
	}

	// remove all occurrences of right from left if any
	static std::string subStrings(std::string left, const std::string& right)
	{
		const auto len = right.length();
		size_t idx;
		while ((idx = left.find(right)) != std::string::npos)
			left.erase(idx, len);
		return left;
	}

public:

	// subscript operators
	cvar& operator[](const cvar& idx) const
	{
		if (this->type == valueType::LIST && listValue)
			return (*listValue)[idx.getInt32()];

		if (this->type == valueType::DICT && dictValue)
			return (*dictValue)[idx];

		throw std::runtime_error("not a list or dictionary");
	}

	cvar& operator[](const int &idx) const
	{
		if (this->type == valueType::LIST && listValue)
			return (*listValue)[idx];

		if (this->type == valueType::DICT && dictValue)
			return (*dictValue)[cvar{ idx }];

		throw std::runtime_error("not a list or dictionary");
	}

	cvar& operator[](const int64_t &idx) const
	{
		if (this->type == valueType::LIST && listValue)
			return (*listValue)[idx];

		if (this->type == valueType::DICT && dictValue)
			return (*dictValue)[cvar{ idx }];

		throw std::runtime_error("not a list or dictionary");
	}

	cvar& operator[](const std::string &idx) const
	{
		if (this->type == valueType::LIST && listValue)
		{
			const auto index = std::stoi(idx);

			if (index < 0)
				throw std::runtime_error("negative index in List");
			if (index == static_cast<int>((*listValue).size()))
				listValue->emplace_back(cvar{});
			if (index > static_cast<int>((*listValue).size()))
				throw std::runtime_error("List index greater than list size");

			return (*listValue)[std::stoi(idx)]; // may throw
		}

		if (this->type == valueType::DICT && dictValue)
			return (*dictValue)[cvar{ idx }];

		throw std::runtime_error("not a list or dictionary");
	}

	cvar& operator[](const char* idx) const
	{
		if (this->type == valueType::LIST && listValue)
		{
			const auto index = std::stoi(idx);

			if (index < 0)
				throw std::runtime_error("negative index in List");
			if (index == static_cast<int>((*listValue).size()))
				listValue->emplace_back(cvar{});
			if (index > static_cast<int>((*listValue).size()))
				throw std::runtime_error("List index greater than list size");

			return (*listValue)[std::stoi(idx)]; // may throw
		}

		if (this->type == valueType::DICT && dictValue)
			return (*dictValue)[cvar{ idx }];

		throw std::runtime_error("not a list or dictionary");
	}

	void copy(const cvar &source)
	{
        clear();
		this->type = source.type;
		this->value = source.value;
		this->valueString = source.valueString;

		if (source.dictValue && this->type == valueType::DICT)
		{
			this->dict();
			*dictValue = *source.dictValue;
		}

		if (source.listValue && this->type == valueType::LIST)
		{
			this->list();
			*listValue = *source.listValue;
		}

		if (source.setValue && this->type == valueType::SET)
		{
			this->set();
			*setValue = *source.setValue;
		}
	}

	cvar* getReference() const
	{
			return (type == valueType::REF && reference) ? reference : nullptr;
	}

	void setReference(cvar* ref) 
	{
		reference = ref;
		type = valueType::REF;
	}

	// assignment operators
	cvar& operator=(const cvar& source)
	{
		copy(source);
		return *this;
	}

	cvar& operator=(cvar&& source) noexcept
	{
        clear();

		this->type = source.type;
		this->value = source.value;
		this->valueString = std::move(source.valueString);

		if (source.dictValue)
		{
			this->dictValue = source.dictValue;
			source.dictValue = nullptr;
		}

		if (source.listValue)
		{
			this->listValue = source.listValue;
			source.listValue = nullptr;
		}

		if (source.setValue)
		{
			this->setValue = source.setValue;
			source.setValue = nullptr;
		}

		source.type = valueType::INT64;
		source.value.asInt64 = 0;

		return *this;
	}

	cvar& operator=(const int& source)
	{
		type = valueType::INT32;
		value.asInt32 = source;
		return *this;
	}

	cvar& operator=(const int64_t& source)
	{
		type = valueType::INT64;
		value.asInt64 = source;
		return *this;
	}

#ifndef _MSC_VER
    // gcc seems to see long long as not being the same as int64_t
    cvar& operator=(const long long int& source)
    {
        type = valueType::INT64;
        value.asInt64 = static_cast<int64_t>(source);
        return *this;
    }
#endif

	cvar& operator=(const double& source)
	{
		type = valueType::DBL;
		value.asDouble = source;
		return *this;
	}

	cvar& operator=(const float& source)
	{
		type = valueType::FLT;
		value.asInt64 = 0;
		value.asFloat = source;
		return *this;
	}

	// interesting... const char* &source fails,
	// and std::string isn't built in, so 
	// the type would convert to bool (which all pointers or
	// numeric types can be treated as bools). 
	// using a not reference (just char*) for string works.
	cvar& operator=(const char* source)
	{
		type = valueType::STR;
		value.asInt64 = 0;
		valueString = source;
		return *this;
	}

	cvar& operator=(const std::string& source)
	{
		type = valueType::STR;
		value.asInt64 = 0;
		valueString = source;
		return *this;
	}

	cvar& operator=(const bool& source)
	{
		type = valueType::BOOL;
		value.asInt64 = 0;
		value.asBool = (source) ? true : false;
		return *this;
	}

	cvar& operator=(const std::vector<cvar>& source)
	{
		if (dictValue)
		{
			delete dictValue;
			dictValue = nullptr;
		}

		if (setValue)
		{
			delete setValue;
			setValue = nullptr;
		}

		list();
		*listValue = source;

		return *this;
	}

	/*cvar& operator=(const std::initializer_list<cvar> args)
	{
		list();
		for (auto a : args)
			listValue->push_back(a);
		return *this;
	}*/

	template<typename T>
	cvar& operator=(const std::unordered_set<T>& source)
	{
		if (dictValue)
		{
			delete dictValue;
			dictValue = nullptr;
		}

		if (listValue)
		{
			delete listValue;
			listValue = nullptr;
		}

		set();
		for (auto &item : source)
			setValue->insert(item);
		return *this;
	}

	template<typename K, typename V>
	cvar& operator=(const std::unordered_map<K, V>& source);

	template<typename K, typename V>
	cvar& operator=(const std::pair<K, V>& source);

	template<typename T>
	cvar& operator=(const std::vector<T>& source)
	{
		list();
		for (auto &item : source)
			listValue->push_back(item);
		return *this;
	}

	/*
	cvar& operator=(const std::vector<std::pair<cvar, cvar>>& source)
	{
		if (listValue)
		{
			delete listValue;
			listValue = nullptr;
		}

		dict();

		for (auto i : source)
			(*dictValue)[i.first] = i.second;

		return *this;
	}
	*/

	int32_t getInt32() const
	{
		if (type == valueType::INT32)
			return value.asInt32;

		switch (type)
		{
		case valueType::INT64:
			return static_cast<int32_t>(value.asInt64);
		case valueType::FLT:
			return static_cast<int32_t>(value.asFloat);
		case valueType::DBL:
			return static_cast<int32_t>(value.asDouble);
		case valueType::BOOL:
			return (value.asBool) ? 1 : 0;
		case valueType::STR:
			return std::strtol(valueString.c_str(), nullptr, 10);
		default:
			return 0;
		}
	}

	operator int32_t()
	{
		lastValue.asInt32 = getInt32();
		return lastValue.asInt32;
	}

	int64_t getInt64() const
	{
		if (type == valueType::INT64)
			return value.asInt64;

		switch (type)
		{
		case valueType::INT32:
			return static_cast<int64_t>(value.asInt32);
		case valueType::FLT:
			return static_cast<int64_t>(value.asFloat);
		case valueType::DBL:
			return static_cast<int64_t>(value.asDouble);
		case valueType::BOOL:
			return (value.asBool) ? 1 : 0;
		case valueType::STR:
			return std::strtoll(valueString.c_str(), nullptr, 10);
		default:
			return 0;
		}
	}

	operator int64_t()
	{
		lastValue.asInt64 = getInt64();
		return lastValue.asInt64;
	}

	float getFloat() const
	{
		if (type == valueType::FLT)
			return value.asFloat;

		switch (type)
		{
		case valueType::INT32:
			return static_cast<float>(value.asInt32);
		case valueType::INT64:
			return static_cast<float>(value.asInt64);
		case valueType::FLT:
			return static_cast<float>(value.asFloat);
		case valueType::DBL:
			return static_cast<float>(value.asDouble);
		case valueType::BOOL:
			return static_cast<float>((value.asBool) ? 1.0 : 0.0);
		case valueType::STR:
			return std::strtof(valueString.c_str(), nullptr);
		default:
			return 0.0f;
		}
	}

	operator float()
	{
		lastValue.asFloat = getFloat();
		return lastValue.asFloat;
	}

	double getDouble() const
	{
		if (type == valueType::DBL)
			return value.asDouble;

		switch (type)
		{
		case valueType::INT32:
			return static_cast<double>(value.asInt32);
		case valueType::INT64:
			return static_cast<double>(value.asInt64);
		case valueType::FLT:
			return static_cast<double>(value.asFloat);
		case valueType::DBL:
			return static_cast<double>(value.asDouble);
		case valueType::BOOL:
			return static_cast<double>((value.asBool) ? 1.0 : 0.0);
		case valueType::STR:
			return std::strtod(valueString.c_str(), nullptr);
		default:
			return 0.0;
		}
	}

	operator double()
	{
		lastValue.asDouble = getDouble();
		return lastValue.asDouble;
	}

	bool getBool() const
	{
		if (type == valueType::BOOL)
			return value.asBool;

		switch (type)
		{
		case valueType::INT32:
			return value.asInt32 != 0 ? true : false;
		case valueType::INT64:
			return value.asInt64 != 0 ? true : false;
		case valueType::FLT:
			return value.asFloat != 0.0 ? true : false;
		case valueType::DBL:
			return value.asDouble != 0.0 ? true : false;
		case valueType::BOOL:
			return value.asBool ? true : false;
		case valueType::STR:
			// no length, "false" or "0" = false, anything else is true
			return (valueString.length() == 0 ||
				valueString == "false" ||
				valueString == "0") ? false : true;
		default:
			return false;
		}
	}

	operator bool()
	{
		lastValue.asBool = getBool();
		return lastValue.asBool;
	}

	std::string getString() const
	{
		if (type == valueType::STR)
			return valueString;

		switch (type)
		{
		case valueType::INT32:
			return std::to_string(value.asInt32);
		case valueType::INT64:
			return std::to_string(value.asInt64);
		case valueType::FLT:
			return trimZeros(std::to_string(value.asFloat));
		case valueType::DBL:
			return trimZeros(std::to_string(value.asDouble));
		case valueType::BOOL:
			return value.asBool ? "true" : "false";
		default:
			return "";
		}
	}

    operator std::string() const
	{
		return  getString();
	}

	std::string* getStringPtr()
	{
		if (type != valueType::STR)
			throw std::runtime_error("getStringPtr can only be called when type is valueType::STR");
		return &valueString;
	}

	// overloads... endless overloads

	bool operator ==(const cvar& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			if (right.type == valueType::DBL || right.type == valueType::FLT)
				return this->getDouble() == right.getDouble();
			return this->getInt64() == right.getInt64();
		case valueType::FLT:
			return this->getFloat() == right.getFloat();
		case valueType::DBL:
			return this->getDouble() == right.getDouble();
		case valueType::BOOL:
			return this->getBool() == right.getBool();
		case valueType::STR:
			if (right.type == valueType::STR)
				return this->valueString == right.valueString;
			else if (right.type == valueType::BOOL)
				return this->getBool() == right.getBool();
			return *this == right.getString();
		// TODO - add list/dict/set comparisions 
		default:
			return false;
		}
	}

	bool operator <(const cvar& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			if (right.type == valueType::DBL || right.type == valueType::FLT)
				return this->getDouble() < right.getDouble();
			return this->getInt64() < right.getInt64();
		case valueType::FLT:
			return *this < right.getFloat();
		case valueType::DBL:
			return *this < right.getDouble();
		case valueType::BOOL:
			return *this < right.getBool();
		case valueType::STR:
			if (this->type == valueType::STR && right.type == valueType::STR)
				return this->valueString < right.valueString;
			return *this < right.getString();
		default:
			return false;
		}
	}

	bool operator >(const cvar& right) const
	{
		return (right < *this);
	}

	bool operator !=(const cvar& right) const
	{
		return !(*this == right);
	}

	bool operator <=(const cvar& right) const
	{
		return !(right < *this);
	}

	bool operator >=(const cvar& right) const
	{
		return !(*this < right);
	}

	// unary operator - (negate)
	cvar operator-() const
	{
		switch (type)
		{
		case valueType::INT32:
			return cvar{ -1 * this->getInt32() };
		case valueType::INT64:
			return cvar{ -1 * this->getInt64() };
		case valueType::FLT:
			return cvar{ -1.0 * this->getFloat() };
		case valueType::DBL:
			return cvar{ -1.0 * this->getDouble() };
		case valueType::BOOL:
			return cvar{ this->getBool() ? false : true };
		case valueType::STR:
			return cvar{ "-"s + this->getString() };
		default:
			return cvar{ 0 };
		}
	}

	int len() const
	{
		if (type == valueType::LIST)
			return listValue ? static_cast<int>(listValue->size()) : 0;
		if (type == valueType::DICT)
			return dictValue ? static_cast<int>(dictValue->size()) : 0;
		if (type == valueType::SET)
			return setValue ? static_cast<int>(setValue->size()) : 0;
		if (type == valueType::STR)
			return static_cast<int>(valueString.length());
		return 0;
	}

	static int len(const cvar& value)
	{
		return value.len();
	}

private:
	void add(const cvar& right) const
	{
		if (type != valueType::SET) // throw
			throw std::runtime_error("left value must of type Set");

		setValue->insert(right);
	}

	void append(const cvar& other) const
	{
		if (other.type == valueType::LIST &&
			type == valueType::LIST) // glue to lists together, return product
		{
			appendList(other);
			return;
		}
		else if (other.type == valueType::DICT &&
			type == valueType::DICT) // throw
		{
			appendDict(other);
			return;
		}
		else if (other.type == valueType::SET &&
			type == valueType::SET) // throw
		{
			appendSet(other);
			return;
		}

		if (type == valueType::LIST)
			listValue->push_back(other);
		else if (type == valueType::SET)
			setValue->insert(other);
		else
			std::runtime_error("left and right types must be the same, or left must be list or set");
	}

	static cvar append(const cvar& left, const cvar& right)
	{
		auto result = left;
		result.append(right);
		return result;
	}

	void appendList(const cvar& other) const
	{	
		if (type != valueType::LIST ||
			other.type != valueType::LIST) // glue to lists together, return product
			throw std::runtime_error("only List containers can be merged with List containers");

		for (auto &item : *other.getList())
			(*this->listValue).push_back(item);
	}

	void appendDict(const cvar& other) const
	{
		if (type != valueType::DICT ||
			other.type != valueType::DICT) // glue to lists together, return product
			throw std::runtime_error("only Dict containers can be merged with Dict containers");

		for (auto &pair : *other.getDict())
			(*this->dictValue)[pair.first] = pair.second;

	}

	void appendSet(const cvar& other) const
	{
		if (type != valueType::SET ||
			other.type != valueType::SET) // glue to lists together, return product
			throw std::runtime_error("only Set types can be merged with Set types");

		for (auto &item : *other.getSet())
			add(item);
	}

	void remove(const cvar& other) const
	{
		if (type == valueType::LIST) // glue to lists together, return product
		{
			removeList(other);
			return;
		}
		else if (type == valueType::DICT)
		{
			removeDict(other);
			return;
		}
		else if (type == valueType::SET) 
		{
			removeSet(other);
			return;
		}

		throw std::runtime_error("left must be a list, dict or set");
	}

	static cvar remove(const cvar& left, const cvar& right)
	{
		auto result = left;
		result.remove(right);
		return result;
	}
	
	void removeSet(const cvar& other) const
	{
		if (type != valueType::SET)
			throw std::runtime_error("set subtraction can only be called on sets");

		if (other.typeof() == valueType::DICT)
			throw std::runtime_error("dictionaries cannot be subtracted from sets");

		if (other.typeof() == valueType::LIST)
		{
			for (auto &i : *(other.getList()))
				if (setValue->count(i))
					setValue->erase(i);				
		}
		else if (other.typeof() == valueType::SET)
		{
			for (auto &i : *(other.getSet()))
				if (setValue->count(i))
					setValue->erase(i);
		}
		else
		{
			if (setValue->count(other))
				setValue->erase(other);
		}
	}

	void removeDict(const cvar& other) const
	{
		if (type != valueType::DICT)
			throw std::runtime_error("dict subtraction can only be called on dicts");

		if (other.typeof() == valueType::DICT)
		{
			for (auto &i : *(other.getDict()))
				if (dictValue->count(i))
					dictValue->erase(i);
		}
		if (other.typeof() == valueType::LIST)
		{
			for (auto &i : *(other.getList()))
				if (dictValue->count(i))
					dictValue->erase(i);
		}
		else if (other.typeof() == valueType::SET)
		{
			for (auto &i : *(other.getSet()))
				if (dictValue->count(i))
					dictValue->erase(i);
		}
		else
		{
			if (dictValue->count(other))
				dictValue->erase(other);
		}
	}

	void removeList(const cvar& other) const
	{
		if (type != valueType::LIST)
			throw std::runtime_error("list subtraction can only be called on lists");

		// the approach here is to make an unordered set
		// of items, or the keys for items, for the `other` variable
		// we will iterate the list, and if the other items are not in
		// the list we will append them

		std::vector<cvar> result;
		std::unordered_set<cvar> otherItems;

		if (other.typeof() == valueType::DICT)
		{
			for (auto &i : *(other.getDict()))
				otherItems.insert(i.first);
			for (auto &i : *listValue)
				if (!otherItems.count(i))
					result.push_back(i);
			*listValue = result;
		}
		if (other.typeof() == valueType::LIST)
		{
			for (auto &i : *(other.getList()))
				otherItems.insert(i);
			for (auto &i : *listValue)
				if (!otherItems.count(i))
					result.push_back(i);
			*listValue = result;
		}
		else if (other.typeof() == valueType::SET)
		{
			for (auto &i : *(other.getSet()))
				otherItems.insert(i);
			for (auto &i : *listValue)
				if (!otherItems.count(i))
					result.push_back(i);
			*listValue = result;
		}
		else
		{
			for (auto &i : *listValue)
				if (i != other)
					result.push_back(i);
			*listValue = result;
		}
	}

public:

	cvar operator+(const cvar& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			if (right.type == valueType::DBL || right.type == valueType::FLT)
				return cvar{ this->getDouble() + right.getDouble() };
			return { *this + right.getInt64() };
		case valueType::FLT:
			return cvar{ *this + right.getFloat() };
		case valueType::DBL:
			return cvar{ *this + right.getDouble() };
		case valueType::BOOL:
			return cvar{ this->getInt64() + right.getInt64() };
		case valueType::STR:
			if (right.type == valueType::STR)
				return cvar{ this->valueString + right.valueString };
			else
				return cvar{ this->getString() + right.getString() };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return append(*this, right);
		default:
			return cvar{ 0 };
		}
	}

	cvar operator+(const int& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			return cvar{ this->getInt64() + right };
		case valueType::BOOL:
			return cvar{ this->getBool() && (right != 0) };
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() + right };
		case valueType::STR:
			return cvar{ this->getString() + std::to_string(right) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return append(*this, cvar{ right });
		default:
			return cvar{ right };
		}
	}

	cvar operator+(const int64_t& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			return cvar{ this->getInt64() + right };
		case valueType::BOOL:
			return cvar{ this->getBool() && (right != 0) };
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() + right };
		case valueType::STR:
			return cvar{ this->getString() + std::to_string(right) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return append(*this, cvar{ right });
		default:
			return cvar{ right };
		}
	}

	cvar operator+(const double& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() + right };
		case valueType::BOOL:
			return cvar{ this->getBool() && (right != 0) };
		case valueType::STR:
			return cvar{ this->getString() + std::to_string(right) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return append(*this, cvar{ right });
		default:
			return cvar{ right };
		}
	}

	cvar operator+(const float& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() + static_cast<double>(right) };
		case valueType::BOOL:
			return cvar{ this->getBool() && (right != 0) };
		case valueType::STR:
			return cvar{ this->getString() + std::to_string(right) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return append(*this, cvar{ right });
		default:
			return cvar{ right };
		}
	}

	cvar operator+(std::string& right) const
	{
		if (type == valueType::LIST || type == valueType::SET)
			return append(*this, cvar{ right });
		if (type == valueType::DICT)
			throw std::runtime_error("Dict types and string types cannot be concatinated");

		return cvar{ this->getString() + right };
	}

	cvar operator+(const std::string& right) const
	{
		if (type == valueType::LIST || type == valueType::SET)
			return append(*this, cvar{ right });
		if (type == valueType::DICT)
			throw std::runtime_error("Dict types and string types cannot be concatinated");

		return cvar{ this->getString() + right };
	}

	cvar operator+(const char* right) const
	{
		if (type == valueType::LIST || type == valueType::SET)
			return append(*this, cvar{ right });
		if (type == valueType::DICT)
			throw std::runtime_error("Dict types and string types cannot be concatinated");

		return cvar{ this->getString() + std::string{right} };
	}

	cvar& operator+=(const cvar& right)
	{
		*this = *this + right;
		return *this;
	}

	cvar& operator+=(const int& right)
	{
		*this = *this + right;
		return *this;
	}

	cvar& operator+=(const int64_t& right)
	{
		*this = *this + right;
		return *this;
	}

#ifndef _MSC_VER
    // gcc seems to see long long as not being the same as int64_t
    cvar& operator+=(const long long int& right)
    {
        *this = *this + static_cast<int64_t>(right);
        return *this;
    }
#endif

	cvar& operator+=(const float& right)
	{
		*this = *this + right;
		return *this;
	}

	cvar& operator+=(const double& right)
	{
		*this = *this + right;
		return *this;
	}

	cvar& operator+=(const std::string& right)
	{
		*this = *this + right;
		return *this;
	}

	cvar& operator+=(const char* right)
	{
		*this = *this + right;
		return *this;
	}

	cvar operator*(const cvar& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			if (right.type == valueType::DBL || right.type == valueType::FLT)
				return cvar{ this->getDouble() * right.getDouble() };
			return cvar{ *this * right.getInt64() };
		case valueType::FLT:
			return cvar{ *this * right.getFloat() };
		case valueType::DBL:
			return cvar{ *this * right.getDouble() };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			throw std::runtime_error("Container types cannot be multiplied");
		default:
			return cvar{ 0 };
		}
	}

	cvar operator*(const int& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			return cvar{ this->getInt64() * right };
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() * right };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			throw std::runtime_error("Container types cannot be multiplied");
		default:
			return cvar{ right };
		}
	}

	cvar operator*(const int64_t& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			return cvar{ this->getInt64() * right };
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() * right };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
			throw std::runtime_error("Container types cannot be multiplied");
		default:
			return cvar{ right };
		}
	}

	cvar operator*(const double& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() * right };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			throw std::runtime_error("Container types cannot be multiplied");
		default:
			return cvar{ right };
		}
	}

	cvar operator*(const float& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() * static_cast<double>(right) };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
			throw std::runtime_error("Container types cannot be multiplied");
		default:
			return cvar{ right };
		}
	}

	cvar operator*(std::string& right) const
	{
		if (type == valueType::LIST ||
			type == valueType::DICT ||
			type == valueType::SET)
			throw std::runtime_error("Container types cannot be multiplied");

		return cvar{ right }; // not right
	}

	cvar operator*(const std::string& right) const
	{
		if (type == valueType::LIST ||
			type == valueType::DICT ||
			type == valueType::SET)
			throw std::runtime_error("Container types cannot be multiplied");

		return cvar{ right }; // not right
	}

	cvar operator*(const char* right) const
	{
		if (type == valueType::LIST ||
			type == valueType::DICT ||
			type == valueType::SET)
			throw std::runtime_error("Container types cannot be multiplied");

		return cvar{ std::string{right} }; // not right
	}


	cvar& operator*=(const cvar& right)
	{
		*this = *this * right;
		return *this;
	}

	cvar& operator*=(const int& right)
	{
		*this = *this * right;
		return *this;
	}

	cvar& operator*=(const int64_t& right)
	{
		*this = *this * right;
		return *this;
	}

	cvar& operator*=(const float& right)
	{
		*this = *this * right;
		return *this;
	}

	cvar& operator*=(const double& right)
	{
		*this = *this * right;
		return *this;
	}

	cvar& operator*=(const std::string& right)
	{
		// TODO smarter
		return *this;
	}

	cvar& operator*=(const char* right)
	{
		// TODO smarter
		return *this;
	}

	cvar operator/(const cvar& right) const
	{
		// divide by zero returns zero
		if (right == 0)	return cvar{ 0 };

		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			if (right.type == valueType::DBL || right.type == valueType::FLT)
			{
				const auto tmp = right.getDouble();
				if (tmp == 0)
					throw std::runtime_error("divide by zero");
				return cvar{ this->getDouble() / tmp };
			}
			{
				const auto tmp = right.getInt64();
				if (tmp == 0)
					throw std::runtime_error("divide by zero");
				return cvar{ *this / tmp };
			}
		case valueType::FLT:
			{
				const auto tmp = right.getFloat();
				if (tmp == 0)
					throw std::runtime_error("divide by zero");
				return cvar{ *this / tmp };
			}
		case valueType::DBL:
			{
				const auto tmp = right.getDouble();
				if (tmp == 0)
					throw std::runtime_error("divide by zero");
				return cvar{ *this / tmp };
			}
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			throw std::runtime_error("Container types are not divisable");
		default:
			return cvar{ 0 };
		}
	}

	cvar operator/(const int& right) const
	{
		// divide by zero returns zero
		if (right == 0)	return cvar{ 0 };

		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			return cvar{ this->getInt64() / right };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() / right };
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			throw std::runtime_error("Container types are not divisable");

		default:
			return cvar{ right };
		}
	}

	cvar operator/(const int64_t& right) const
	{
		// divide by zero returns zero
		if (right == 0)	return cvar{ 0 };

		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			return cvar{ this->getInt64() / right };
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() / right };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			throw std::runtime_error("Container types are not divisable");
		default:
			return cvar{ right };
		}
	}

	cvar operator/(const double& right) const
	{
		// divide by zero returns zero
		if (right == 0)	return cvar{ 0 };

		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() / right };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			throw std::runtime_error("Container types are not divisable");
		default:
			return cvar{ right };
		}
	}

	cvar operator/(const float& right) const
	{
		// divide by zero returns zero
		if (right == 0)	return cvar{ 0 };

		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() / static_cast<double>(right) };
		case valueType::BOOL:
			return cvar{ right }; // todo - make smarter
		case valueType::STR:
			return cvar{ right }; // todo - make smarter
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			throw std::runtime_error("Container types are not divisable");
		default:
			return cvar{ right };
		}
	}

	cvar operator/(std::string& right) const
	{
		if (type == valueType::LIST ||
			type == valueType::DICT ||
			type == valueType::SET)
			throw std::runtime_error("Container types cannot be multiplied");
		return cvar{ right }; // not right
	}

	cvar operator/(const std::string& right) const
	{
		if (type == valueType::LIST ||
			type == valueType::DICT ||
			type == valueType::SET)
			throw std::runtime_error("Container types cannot be multiplied");
		return cvar{ right }; // not right
	}

	cvar operator/(const char* right) const
	{
		if (type == valueType::LIST ||
			type == valueType::DICT ||
			type == valueType::SET)
			throw std::runtime_error("Container types cannot be multiplied");
		return cvar{ std::string{ right } }; // not right
	}


	cvar& operator/=(const cvar& right)
	{
		*this = *this / right;
		return *this;
	}

	cvar& operator/=(const int& right)
	{
		*this = *this / right;
		return *this;
	}

	cvar& operator/=(const int64_t& right)
	{
		*this = *this / right;
		return *this;
	}

	cvar& operator/=(const float& right)
	{
		*this = *this / right;
		return *this;
	}

	cvar& operator/=(const double& right)
	{
		*this = *this / right;
		return *this;
	}

	cvar& operator/=(const std::string& right)
	{
		// TODO smarter
		return *this;
	}

	cvar& operator/=(const char* right)
	{
		// TODO smarter
		return *this;
	}


	cvar operator-(const cvar& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
			if (right.type == valueType::DBL || right.type == valueType::FLT)
				return cvar{ this->getDouble() - right.getDouble() };
			return cvar{ *this - right.getInt64() };
		case valueType::FLT:
			return cvar{ *this - right.getFloat() };
		case valueType::DBL:
			return cvar{ *this - right.getDouble() };
		case valueType::BOOL:
			return cvar{ this->getInt64() + right.getInt64() };
		case valueType::STR:
			if (this->type == valueType::STR && right.type == valueType::STR)
				return cvar{ subStrings(this->valueString, right.valueString) };
			else
				return cvar{ subStrings(this->getString(), right.getString()) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return remove(*this, cvar{ right });

		default:
			return cvar{ 0 };
		}
	}

	cvar operator-(const int& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::BOOL:
			return cvar{ this->getInt64() - right };
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() - right };
		case valueType::STR:
			return cvar{ subStrings(this->getString(), std::to_string(right)) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return remove(*this, cvar{ right });
		default:
			return cvar{ right };
		}
	}

	cvar operator-(const int64_t& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::BOOL:
			return cvar{ this->getInt64() - right };
		case valueType::FLT:
		case valueType::DBL:
			return cvar{ this->getDouble() - right };
		case valueType::STR:
			return cvar{ subStrings(this->getString(), std::to_string(right)) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return remove(*this, cvar{ right });
		default:
			return cvar{ right };
		}
	}

	cvar operator-(const double& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::FLT:
		case valueType::DBL:
		case valueType::BOOL:
			return cvar{ this->getDouble() - right };
		case valueType::STR:
			return cvar{ subStrings(this->getString(), std::to_string(right)) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return remove(*this, cvar{ right });
		default:
			return cvar{ right };
		}
	}

	cvar operator-(const float& right) const
	{
		switch (type)
		{
		case valueType::INT32:
		case valueType::INT64:
		case valueType::FLT:
		case valueType::DBL:
		case valueType::BOOL:
			return cvar{ this->getDouble() - static_cast<double>(right) };
		case valueType::STR:
			return cvar{ subStrings(this->getString(), std::to_string(right)) };
		case valueType::LIST:
		case valueType::DICT:
		case valueType::SET:
			return remove(*this, cvar{ right });
		default:
			return cvar{ right };
		}
	}

	cvar operator-(std::string& right) const
	{
		if (type == valueType::LIST || 
			type == valueType::DICT ||
			type == valueType::SET)
			return remove(*this, cvar{ right });
		return cvar{ subStrings(this->getString(), right) };
	}

	cvar operator-(const std::string& right) const
	{
		if (type == valueType::LIST || 
			type == valueType::DICT ||
			type == valueType::SET)
			return remove(*this, cvar{ right });
		return cvar{ subStrings(this->getString(), right) };
	}

	cvar operator-(const char* right) const
	{
		if (type == valueType::LIST || 
			type == valueType::DICT ||
			type == valueType::SET)
			return remove(*this, cvar{ right });
		return cvar{ subStrings(this->getString(), std::string{right}) };
	}

	cvar& operator-=(const cvar& right)
	{
		*this = *this - right;
		return *this;
	}

	cvar& operator-=(const int& right)
	{
		*this = *this - right;
		return *this;
	}

	cvar& operator-=(const int64_t& right)
	{
		*this = *this - right;
		return *this;
	}

	cvar& operator-=(const float& right)
	{
		*this = *this - right;
		return *this;
	}

	cvar& operator-=(const double& right)
	{
		*this = *this - right;
		return *this;
	}

	cvar& operator-=(const std::string& right)
	{
		*this = *this - right;
		return *this;
	}

	cvar& operator-=(const char* right)
	{
		*this = *this - right;
		return *this;
	}

	// << overload for std::ostream 
	friend std::ostream& operator<<(std::ostream& os, const cvar& source)
	{
		const auto result = source.getString();
		os << result;
		return os;
	}

	// == operator overloads for all the POD types
	friend bool operator ==(const int& left, const cvar& right);
	friend bool operator ==(const cvar& left, const int& right);
	friend bool operator ==(const int64_t& left, const cvar& right);
	friend bool operator ==(const cvar& left, const int64_t& right);
#ifndef _MSC_VER
    friend bool operator ==(const long long int& left, const cvar& right);
    friend bool operator ==(const cvar& left, const long long int& right);
#endif
	friend bool operator ==(const float& left, const cvar& right);
	friend bool operator ==(const cvar& left, const float& right);
	friend bool operator ==(const double& left, const cvar& right);
	friend bool operator ==(const cvar& left, const double& right);
	friend bool operator ==(const bool& left, const cvar& right);
	friend bool operator ==(const cvar& left, const bool& right);
	friend bool operator ==(const std::string& left, const cvar& right);
	friend bool operator ==(const cvar& left, const std::string& right);
	friend bool operator ==(const char* left, const cvar& right);
	friend bool operator ==(const cvar& left, const char* right);

	// != operator overloads for all the POD types (uses the == overload)
	friend bool operator !=(const int& left, const cvar& right);
	friend bool operator !=(const cvar& left, const int& right);
	friend bool operator !=(const int64_t& left, const cvar& right);
	friend bool operator !=(const cvar& left, const int64_t& right);
#ifndef _MSC_VER
    friend bool operator !=(const long long int& left, const cvar& right);
    friend bool operator !=(const cvar& left, const long long int& right);
#endif
	friend bool operator !=(const float& left, const cvar& right);
	friend bool operator !=(const cvar& left, const float& right);
	friend bool operator !=(const double& left, const cvar& right);
	friend bool operator !=(const cvar& left, const double& right);
	friend bool operator !=(const bool& left, const cvar& right);
	friend bool operator !=(const cvar& left, const bool& right);
	friend bool operator !=(const std::string& left, const cvar& right);
	friend bool operator !=(const cvar& left, const std::string& right);
	friend bool operator !=(const char* left, const cvar& right);
	friend bool operator !=(const cvar& left, const char* right);

	// < operator overloads for all the POD types
	friend bool operator <(const int& left, const cvar& right);
	friend bool operator <(const cvar& left, const int& right);
	friend bool operator <(const int64_t& left, const cvar& right);
	friend bool operator <(const cvar& left, const int64_t& right);
	friend bool operator <(const float& left, const cvar& right);
	friend bool operator <(const cvar& left, const float& right);
	friend bool operator <(const double& left, const cvar& right);
	friend bool operator <(const cvar& left, const double& right);
	friend bool operator <(const bool& left, const cvar& right);
	friend bool operator <(const cvar& left, const bool& right);
	friend bool operator <(const std::string& left, const cvar& right);
	friend bool operator <(const cvar& left, const std::string& right);

	// <= operator overloads for all the POD types
	friend bool operator <=(const int& left, const cvar& right);
	friend bool operator <=(const cvar& left, const int& right);
	friend bool operator <=(const int64_t& left, const cvar& right);
	friend bool operator <=(const cvar& left, const int64_t& right);
	friend bool operator <=(const float& left, const cvar& right);
	friend bool operator <=(const cvar& left, const float& right);
	friend bool operator <=(const double& left, const cvar& right);
	friend bool operator <=(const cvar& left, const double& right);
	friend bool operator <=(const bool& left, const cvar& right);
	friend bool operator <=(const cvar& left, const bool& right);
	friend bool operator <=(const std::string& left, const cvar& right);
	friend bool operator <=(const cvar& left, const std::string& right);

	// > operator overloads for all the POD types
	friend bool operator >(const int& left, const cvar& right);
	friend bool operator >(const cvar& left, const int& right);
	friend bool operator >(const int64_t& left, const cvar& right);
	friend bool operator >(const cvar& left, const int64_t& right);
	friend bool operator >(const float& left, const cvar& right);
	friend bool operator >(const cvar& left, const float& right);
	friend bool operator >(const double& left, const cvar& right);
	friend bool operator >(const cvar& left, const double& right);
	friend bool operator >(const bool& left, const cvar& right);
	friend bool operator >(const cvar& left, const bool& right);
	friend bool operator >(const std::string& left, const cvar& right);
	friend bool operator >(const cvar& left, const std::string& right);

	// >= operator overloads for all the POD types
	friend bool operator >=(const int& left, const cvar& right);
	friend bool operator >=(const cvar& left, const int& right);
	friend bool operator >=(const int64_t& left, const cvar& right);
	friend bool operator >=(const cvar& left, const int64_t& right);
	friend bool operator >=(const float& left, const cvar& right);
	friend bool operator >=(const cvar& left, const float& right);
	friend bool operator >=(const double& left, const cvar& right);
	friend bool operator >=(const cvar& left, const double& right);
	friend bool operator >=(const bool& left, const cvar& right);
	friend bool operator >=(const cvar& left, const bool& right);
	friend bool operator >=(const std::string& left, const cvar& right);
	friend bool operator >=(const cvar& left, const std::string& right);

	// + operator overloads for all the POD types
	friend cvar operator+(const int& left, const cvar& right);
	friend cvar operator+(const int64_t& left, const cvar& right);
	friend cvar operator+(const float& left, const cvar& right);
	friend cvar operator+(const double& left, const cvar& right);
	friend cvar operator+(const bool& left, const cvar& right);
	friend cvar operator+(const std::string& left, const cvar& right);
	friend cvar operator+(const char* left, const cvar& right);

	// - operator overloads for all the POD types
	friend cvar operator-(const int& left, const cvar& right);
	friend cvar operator-(const int64_t& left, const cvar& right);
	friend cvar operator-(const float& left, const cvar& right);
	friend cvar operator-(const double& left, const cvar& right);
	friend cvar operator-(const bool& left, const cvar& right);
	friend cvar operator-(const std::string& left, const cvar& right);
	friend cvar operator-(const char* left, const cvar& right);
};

template <typename K, typename V>
cvar& cvar::operator=(const std::unordered_map<K, V>& source)
{
	if (setValue)
	{
		delete setValue;
		setValue = nullptr;
	}

	if (listValue)
	{
		delete listValue;
		listValue = nullptr;
	}

	dict();
	for (const std::pair<const K,V>& item : source)
		(*dictValue)[cvar{ item.first }] = cvar{ item.second };
	return *this;
}

template <typename K, typename V>
cvar& cvar::operator=(const std::pair<K, V>& source)
{
	dict();
	(*dictValue)[cvar{ source.first }] = cvar{ source.second };
	return *this;
}

inline bool operator ==(const int& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
		return left == right.getInt32();
	case cvar::valueType::INT64:
		return left == right.getInt64();
	case cvar::valueType::FLT:
		return static_cast<float>(left) == right.getFloat();
	case cvar::valueType::DBL:
		return static_cast<double>(left) == right.getDouble();
	case cvar::valueType::BOOL:
		return (left != 0) == right.getBool();
	case cvar::valueType::STR:
		return std::to_string(left) == right.valueString;
	default:
		return false;
	}
}

inline bool operator ==(const cvar& left, const int& right)
{
	switch (left.type)
	{
	case cvar::valueType::INT32:
		return left.getInt32() == right;
	case cvar::valueType::INT64:
		return left.getInt64() == right;
	case cvar::valueType::FLT:
		return left.getFloat() == static_cast<float>(right);
	case cvar::valueType::DBL:
		return left.getDouble() == static_cast<double>(right);
	case cvar::valueType::BOOL:
		return left == (right != 0);
	case cvar::valueType::STR:
		return left.valueString == std::to_string(right);
	default:
		return false;
	}
}

inline bool operator ==(const int64_t& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
		return left == right.getInt64();
	case cvar::valueType::FLT:
		return static_cast<float>(left) == right.getFloat();
	case cvar::valueType::DBL:
		return static_cast<double>(left) == right.getDouble();
	case cvar::valueType::BOOL:
		return (left != 0) == right.getBool();
	case cvar::valueType::STR:
		return std::to_string(left) == right.valueString;
	default:
		return false;
	}
}

inline bool operator ==(const cvar& left, const int64_t& right)
{
	switch (left.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
		return left.getInt64() == right;
	case cvar::valueType::FLT:
		return left.getFloat() == static_cast<float>(right);
	case cvar::valueType::DBL:
		return left.getDouble() == static_cast<double>(right);
	case cvar::valueType::BOOL:
		return left == (right != 0);
	case cvar::valueType::STR:
		return left.valueString == std::to_string(right);
	default:
		return false;
	}
}

#ifndef _MSC_VER
inline bool operator ==(const long long int& left, const cvar& right)
{
    switch (right.type)
    {
    case cvar::valueType::INT32:
    case cvar::valueType::INT64:
        return static_cast<int64_t>(left) == right.getInt64();
    case cvar::valueType::FLT:
        return static_cast<float>(left) == right.getFloat();
    case cvar::valueType::DBL:
        return static_cast<double>(left) == right.getDouble();
    case cvar::valueType::BOOL:
        return (left != 0) == right.getBool();
    case cvar::valueType::STR:
        return std::to_string(left) == right.valueString;
    default:
        return false;
    }
}

inline bool operator ==(const cvar& left, const long long int& right)
{
    switch (left.type)
    {
    case cvar::valueType::INT32:
    case cvar::valueType::INT64:
        return left.getInt64() == static_cast<int64_t>(right);
    case cvar::valueType::FLT:
        return left.getFloat() == static_cast<float>(right);
    case cvar::valueType::DBL:
        return left.getDouble() == static_cast<double>(right);
    case cvar::valueType::BOOL:
        return left == (right != 0);
    case cvar::valueType::STR:
        return left.valueString == std::to_string(right);
    default:
        return false;
    }
}

#endif

inline bool operator ==(const float& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
		return left == right.getFloat();
	case cvar::valueType::DBL:
		return static_cast<double>(left) == right.getDouble();
	case cvar::valueType::BOOL:
		return (left != 0) == right.getBool();
	case cvar::valueType::STR:
		return left == right.getFloat();
	default:
		return false;
	}
}

inline bool operator ==(const cvar& left, const float& right)
{
	switch (left.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
		return left.getFloat() == right;
	case cvar::valueType::DBL:
		return left.getDouble() == static_cast<double>(right);
	case cvar::valueType::BOOL:
		return left == (right != 0);
	case cvar::valueType::STR:
		return left.getFloat() == right;
	default:
		return false;
	}
}

inline bool operator ==(const double& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return left == right.getDouble();
	case cvar::valueType::BOOL:
		return (left != 0) == right.getBool();
	case cvar::valueType::STR:
		return left == right.getDouble();
	default:
		return false;
	}
}

inline bool operator ==(const cvar& left, const double& right)
{
	switch (left.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return left.getDouble() == right;
	case cvar::valueType::BOOL:
		return left == (right != 0);
	case cvar::valueType::STR:
		return left.getDouble() == right;
	default:
		return false;
	}
}

inline bool operator ==(const bool& left, const cvar& right)
{
	return left == right.getBool();
}

inline bool operator ==(const cvar& left, const bool& right)
{
	return left.getBool() == right;
}

inline bool operator ==(const std::string& left, const cvar& right)
{
	return left == right.getString();
}

inline bool operator ==(const cvar& left, const std::string& right)
{
	return left.getString() == right;
}

inline bool operator ==(const cvar& left, const char* right)
{
	const auto state =
		(strcmp(right, "0") == 0 ||
			strcmp(right, "false") == 0 ||
			strlen(right) == 0) ? false : true;
	return left.getBool() == state;
}

inline bool operator ==(const char* left, const cvar& right)
{
	const auto state =
		(strcmp(left, "0") == 0 ||
			strcmp(left, "false") == 0 ||
			strlen(left) == 0) ? false : true;
	return state == right.getBool();
}

inline bool operator !=(const int& left, const cvar& right)
{
	return !(left == right);
}

inline bool operator !=(const cvar& left, const int& right)
{
	return !(left == right);
}

inline bool operator !=(const int64_t& left, const cvar& right)
{
	return !(left == right);
}

inline bool operator !=(const cvar& left, const int64_t& right)
{
	return !(left == right);
}

#ifndef _MSC_VER
inline bool operator !=(const long long int& left, const cvar& right)
{
    return !(static_cast<int64_t>(left) == right);
}

inline bool operator !=(const cvar& left, const long long int& right)
{
    return !(left == static_cast<int64_t>(right));
}
#endif

inline bool operator !=(const float& left, const cvar& right)
{
	return !(left == right);
}

inline bool operator !=(const cvar& left, const float& right)
{
	return !(left == right);
}

inline bool operator !=(const double& left, const cvar& right)
{
	return !(left == right);
}

inline bool operator !=(const cvar& left, const double& right)
{
	return !(left == right);
}

inline bool operator !=(const bool& left, const cvar& right)
{
	return !(left == right);
}

inline bool operator !=(const cvar& left, const bool& right)
{
	return !(left == right);
}

inline bool operator !=(const std::string& left, const cvar& right)
{
	return !(left == right);
}

inline bool operator !=(const cvar& left, const std::string& right)
{
	return !(left == right);
}

inline bool operator !=(const char* left, const cvar& right)
{
	return !(left == right);
}

inline bool operator !=(const cvar& left, const char* right)
{
	return !(left == right);
}

inline bool operator <(const int& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
		return left < right.getInt32();
	case cvar::valueType::INT64:
		return left < right.getInt64();
	case cvar::valueType::FLT:
		return static_cast<float>(left) < right.getFloat();
	case cvar::valueType::DBL:
		return static_cast<double>(left) < right.getDouble();
	case cvar::valueType::BOOL:
		throw std::runtime_error("< operator used boolean value");
	case cvar::valueType::STR:
		return std::to_string(left) < right.valueString;
	default:
		return false;
	}
}

inline bool operator <(const cvar& left, const int& right)
{
	switch (left.type)
	{
	case cvar::valueType::INT32:
		return left.getInt32() < right;
	case cvar::valueType::INT64:
		return left.getInt64() < right;
	case cvar::valueType::FLT:
		return left.getFloat() < static_cast<float>(right);
	case cvar::valueType::DBL:
		return left.getDouble() < static_cast<double>(right);
	case cvar::valueType::BOOL:
		throw std::runtime_error("< operator used boolean value");
	case cvar::valueType::STR:
		return left.valueString < std::to_string(right);
	default:
		return false;
	}
}

inline bool operator <(const int64_t& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
		return left < right.getInt64();
	case cvar::valueType::FLT:
		return static_cast<float>(left) < right.getFloat();
	case cvar::valueType::DBL:
		return static_cast<double>(left) < right.getDouble();
	case cvar::valueType::BOOL:
		throw std::runtime_error("< operator used boolean value");
	case cvar::valueType::STR:
		return std::to_string(left) < right.valueString;
	default:
		return false;
	}
}

inline bool operator <(const cvar& left, const int64_t& right)
{
	switch (left.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
		return left.getInt64() < right;
	case cvar::valueType::FLT:
		return left.getFloat() < static_cast<float>(right);
	case cvar::valueType::DBL:
		return left.getDouble() < static_cast<double>(right);
	case cvar::valueType::BOOL:
		throw std::runtime_error("< operator used boolean value");
	case cvar::valueType::STR:
		return left.valueString < std::to_string(right);
	default:
		return false;
	}
}

inline bool operator <(const float& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
		return left < right.getFloat();
	case cvar::valueType::DBL:
		return static_cast<double>(left) < right.getDouble();
	case cvar::valueType::BOOL:
		throw std::runtime_error("< operator used boolean value");
	case cvar::valueType::STR:
		return std::to_string(left) < right.valueString;
	default:
		return false;
	}
}

inline bool operator <(const cvar& left, const float& right)
{
	switch (left.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
		return left.getFloat() < right;
	case cvar::valueType::DBL:
		return left.getDouble() < static_cast<double>(right);
	case cvar::valueType::BOOL:
		throw std::runtime_error("< operator used boolean value");
	case cvar::valueType::STR:
		return left.valueString < std::to_string(right);
	default:
		return false;
	}
}

inline bool operator <(const double& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return left < right.getDouble();
	case cvar::valueType::BOOL:
		throw std::runtime_error("< operator used boolean value");
	case cvar::valueType::STR:
		return std::to_string(left) < right.valueString;
	default:
		return false;
	}
}

inline bool operator <(const cvar& left, const double& right)
{
	switch (left.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return left.getDouble() < right;
	case cvar::valueType::BOOL:
		throw std::runtime_error("< operator used boolean value");
	case cvar::valueType::STR:
		return left.valueString < std::to_string(right);
	default:
		return false;
	}
}

inline bool operator <(const bool& left, const cvar& right)
{
	return left < right.getBool();
}

inline bool operator <(const cvar& left, const bool& right)
{
	return left.getInt64() < static_cast<int64_t>(right);
}

inline bool operator <(const std::string& left, const cvar& right)
{
	return left < right.getString();
}

inline bool operator <(const cvar& left, const std::string& right)
{
	return left.getString() < right;
}

inline bool operator <=(const int& left, const cvar& right)
{
	return !(right < left);
}

inline bool operator <=(const cvar& left, const int& right)
{
	return !(right < left);
}

inline bool operator <=(const int64_t& left, const cvar& right)
{
	return !(right < left);
}

inline bool operator <=(const cvar& left, const int64_t& right)
{
	return !(right < left);
}

inline bool operator <=(const float& left, const cvar& right)
{
	return !(right < left);
}

inline bool operator <=(const cvar& left, const float& right)
{
	return !(right < left);
}

inline bool operator <=(const double& left, const cvar& right)
{
	return !(right < left);
}

inline bool operator <=(const cvar& left, const double& right)
{
	return !(right < left);
}

inline bool operator <=(const bool& left, const cvar& right)
{
	return !(right < left);
}

inline bool operator <=(const cvar& left, const bool& right)
{
	return !(right < left);
}

inline bool operator <=(const std::string& left, const cvar& right)
{
	return !(right < left);
}

inline bool operator <=(const cvar& left, const std::string& right)
{
	return !(right < left);
}

inline bool operator >(const int& left, const cvar& right)
{
	return (right < left);
}

inline bool operator >(const cvar& left, const int& right)
{
	return (right < left);
}

inline bool operator >(const int64_t& left, const cvar& right)
{
	return (right < left);
}

inline bool operator >(const cvar& left, const int64_t& right)
{
	return (right < left);
}

inline bool operator >(const float& left, const cvar& right)
{
	return (right < left);
}

inline bool operator >(const cvar& left, const float& right)
{
	return (right < left);
}

inline bool operator >(const double& left, const cvar& right)
{
	return (right < left);
}

inline bool operator >(const cvar& left, const double& right)
{
	return (right < left);
}

inline bool operator >(const bool& left, const cvar& right)
{
	return (right < left);
}

inline bool operator >(const cvar& left, const bool& right)
{
	return (right < left);
}

inline bool operator >(const std::string& left, const cvar& right)
{
	return (right < left);
}

inline bool operator >(const cvar& left, const std::string& right)
{
	return (right < left);
}

inline bool operator >=(const int& left, const cvar& right)
{
	return !(left < right);
}

inline bool operator >=(const cvar& left, const int& right)
{
	return !(left < right);
}

inline bool operator >=(const int64_t& left, const cvar& right)
{
	return !(left < right);
}

inline bool operator >=(const cvar& left, const int64_t& right)
{
	return !(left < right);
}

inline bool operator >=(const float& left, const cvar& right)
{
	return !(left < right);
}

inline bool operator >=(const cvar& left, const float& right)
{
	return !(left < right);
}

inline bool operator >=(const double& left, const cvar& right)
{
	return !(left < right);
}

inline bool operator >=(const cvar& left, const double& right)
{
	return !(left < right);
}

inline bool operator >=(const bool& left, const cvar& right)
{
	return !(left < right);
}

inline bool operator >=(const cvar& left, const bool& right)
{
	return !(left < right);
}

inline bool operator >=(const std::string& left, const cvar& right)
{
	return !(left < right);
}

inline bool operator >=(const cvar& left, const std::string& right)
{
	return !(left < right);
}

inline cvar operator+(const int& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
		if (right.type == cvar::valueType::DBL || right.type == cvar::valueType::FLT)
			return cvar{ static_cast<double>(left) + right.getDouble() };
		return cvar{ left + right.getInt64() };
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return cvar{ left + right.getDouble() };
	case cvar::valueType::BOOL:
		return cvar{ left && right.getBool() };
	case cvar::valueType::STR:
		return cvar{ std::to_string(left) + right.valueString };
	default:
		return cvar{ right };
	}
}

inline cvar operator+(const int64_t& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
		if (right.type == cvar::valueType::DBL || right.type == cvar::valueType::FLT)
			return cvar{ static_cast<double>(left) + right.getDouble() };
		return cvar{ left + right.getInt64() };
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return cvar{ left + right.getDouble() };
	case cvar::valueType::BOOL:
		return cvar{ left && right.getBool() };
	case cvar::valueType::STR:
		return cvar{ std::to_string(left) + right.valueString };
	default:
		return cvar{ right };
	}
}

inline cvar operator+(const float& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return cvar{ static_cast<double>(left) + right.getDouble() };
	case cvar::valueType::BOOL:
		return cvar{ left && right.getBool() };
	case cvar::valueType::STR:
		return cvar{ std::to_string(left) + right.valueString };
	default:
		return cvar{ right };
	}
}

inline cvar operator+(const double& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return cvar{ left + right.getDouble() };
	case cvar::valueType::BOOL:
		return cvar{ left && right.getBool() };
	case cvar::valueType::STR:
		return cvar{ std::to_string(left) + right.valueString };
	default:
		return cvar{ right };
	}
}

inline cvar operator+(const bool& left, const cvar& right)
{
	return cvar{ left && right.getBool() };
}

inline cvar operator+(const std::string& left, const cvar& right)
{
	return cvar{ left + right.getString() };
}

inline cvar operator+(const char* left, const cvar& right)
{
	return cvar{ std::string{left} + right.getString() };
}

inline cvar operator-(const int& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
		if (right.type == cvar::valueType::DBL || right.type == cvar::valueType::FLT)
			return cvar{ static_cast<double>(left) - right.getDouble() };
		return cvar{ left - right.getInt64() };
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return cvar{ left - right.getDouble() };
	case cvar::valueType::BOOL:
		return cvar{ left || right.getBool() };
	case cvar::valueType::STR:
		return cvar{ right.subStrings(std::to_string(left), right.valueString) };
	default:
		return cvar{ right };
	}
}

inline cvar operator-(const int64_t& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
		if (right.type == cvar::valueType::DBL || right.type == cvar::valueType::FLT)
			return cvar{ static_cast<double>(left) + right.getDouble() };
		return cvar{ left + right.getInt64() };
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return cvar{ left + right.getDouble() };
	case cvar::valueType::BOOL:
		return cvar{ left && right.getBool() };
	case cvar::valueType::STR:
		return cvar{ right.subStrings(std::to_string(left), right.valueString) };
	default:
		return cvar{ right };
	}
}

inline cvar operator-(const float& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return cvar{ static_cast<double>(left) + right.getDouble() };
	case cvar::valueType::BOOL:
		return cvar{ left && right.getBool() };
	case cvar::valueType::STR:
		return cvar{ right.subStrings(std::to_string(left), right.valueString) };
	default:
		return cvar{ right };
	}
}

inline cvar operator-(const double& left, const cvar& right)
{
	switch (right.type)
	{
	case cvar::valueType::INT32:
	case cvar::valueType::INT64:
	case cvar::valueType::FLT:
	case cvar::valueType::DBL:
		return cvar{ left + right.getDouble() };
	case cvar::valueType::BOOL:
		return cvar{ left && right.getBool() };
	case cvar::valueType::STR:
		return cvar{ right.subStrings(std::to_string(left), right.valueString) };
	default:
		return cvar{ right };
	}
}

inline cvar operator-(const bool& left, const cvar& right)
{
	return cvar{ left || right.getBool() };
}

inline cvar operator-(const std::string& left, const cvar& right)
{
	return cvar{ right.subStrings(left, right.getString()) };
}

inline cvar operator-(const char* left, const cvar& right)
{
	return cvar{ right.subStrings(std::string{left}, right.getString()) };
}

inline cvar operator""_cvar(const unsigned long long val)
{
	return cvar{ static_cast<int64_t>(val) };
}

inline cvar operator""_cvar(const long double val)
{
	return cvar{ static_cast<double>(val) };
}

inline cvar operator""_cvar(const char* text, std::size_t size)
{
	return cvar{ std::string{text} };
}
