#ifndef RARELOGIC_CPPLIB_STR_STRTOOLS_H_
#define RARELOGIC_CPPLIB_STR_STRTOOLS_H_

#include "../include/libcommon.h"

#include <vector>
#include <unordered_set>
#include <algorithm>

// __strlist a define for a vector of c_strings
typedef std::vector<char*>* __strList;

// __stringlist a define for a vector of strings
typedef std::vector<std::string>* __stringList;

std::string N2S(int32_t Value, int32_t MinWidth = 0);
std::string N2S(int64_t Value, int32_t MinWidth = 0);
void N2S(int64_t Value, std::string& Result);

// makes a heap copy of a const string... remember to "delete []<string>" after use
char* copyStr(const char* SourceStr);

// copy the source string into the destination, respecting the maximum allowed length
void copyStr(char* dest, const char* source, int32_t maxLen);

// Cleans a string in place (must be char*, if const char* then use copyStr)
void cleanStr(char* Str, char CleanChar = ' ');

std::string cleanStr(std::string String, std::string Remove);

std::string join(const std::vector<std::string>& strings, std::string quotes = "");
std::string join(const std::unordered_set<std::string>& strings, std::string quotes = "");

inline std::string trim(std::string text, std::string whiteSpace)
{
	auto end = text.find_last_not_of(whiteSpace);

	if (end != std::string::npos)
		text.resize(end + 1);

	auto start = text.find_first_not_of(whiteSpace);

	if (start != std::string::npos)
		text = text.substr(start);

	start = text.find_first_not_of(whiteSpace);
	end = text.find_last_not_of(whiteSpace);

	if (start == end && end == std::string::npos)
		text.clear();

	return text;
}

// real non copying non proper split
std::vector<std::string> split(const std::string& Source, char Token);
// real version with re-use
void split(const std::string& Source, char Token, std::vector<std::string>& Result);

// use on non cost data, modifies original string, make a copy. Remember to "delete <__strlist>" after use
// return empty set if empty string provided.
__strList splitStr(char* SourceStr, char* SplitChars);

// use on non cost data, modifies original string, make a copy. Remember to "delete <__strlist>" after use
// return empty set if empty string provided.
void splitStr(const std::string& SourceStr, std::string SplitChars, __stringList Result);

// use on non cost data, modifies original string, make a copy. Remember to "delete <__strlist>" after use
// return empty set if empty string provided.
/*void splitStr( std::string SourceStr, char* SplitChars, __stringList Result )
{
	std::string Splits = SplitChars;
	splitStr( SourceStr, Splits, Result );

};*/

// Convert a std::string into uppercase characters
void toUpper(std::string& Text);

// Convert a std::string into uppercase characters
void toLower(std::string& Text);
void toLower(char* str);

std::string char2hex(char dec);

void Replace(std::string& Source, std::string Find, std::string Replace);
bool EndsWith(std::string Source, std::string Find);
bool StartsWith(const std::string& Source, const std::string& Find);

#endif // RARELOGIC_CPPLIB_STR_STRTOOLS_H_
