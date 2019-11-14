#include "strtools.h"

bool EndsWith(std::string Source, std::string Find)
{
    const auto pos = Source.rfind(Find);

    if (pos == std::string::npos)
        return false;

    if (pos == Source.length() - Find.length())
        return true;

    return false;
}

bool StartsWith(const std::string& Source, const std::string& Find)
{
    return Source.length() >= Find.length() && std::equal(Find.begin(), Find.end(), Source.begin());
}

void Replace(std::string& Source, std::string Find, std::string Replace)
{
    size_t pos = 0;

    while (true)
    {
        pos = Source.find(Find, pos);

        if (pos == std::string::npos)
            return;

        Source.erase(pos, Find.length());

        Source.insert(pos, Replace);

        pos += Replace.length();
    }
}

// Removes white space in strings... cleans in place, moves null termator
void cleanStr(char* Str, char CleanChar)
{
    char* ReadPtr = Str;
    char* WritePtr = Str;

    while (*ReadPtr)
    {
        *WritePtr = *ReadPtr;
        WritePtr++;
        ReadPtr++;
        if ((*ReadPtr == CleanChar) && (*(WritePtr - 1) == CleanChar))
            WritePtr--;
    }

    *WritePtr = 0;
}

std::string join(const std::vector<std::string>& strings, std::string quotes)
{
    if (strings.empty())
        return "";

    std::string res;

    bool comma = false;
    for (auto& str : strings)
    {
        if (str.empty())
            continue;

        res += (comma ? "," : "") + quotes + str + quotes;
        comma = true;
    }

    return res;
}

std::string join(const std::unordered_set<std::string>& strings, std::string quotes)
{
    if (strings.empty())
        return "";

    std::string res;

    bool comma = false;
    for (auto& str : strings)
    {
        if (str.empty())
            continue;

        res += (comma ? "," : "") + quotes + str + quotes;
        comma = true;
    }

    return res;
}

std::string cleanStr(std::string Source, std::string Remove)
{
    std::string Result = "";

    const char* Start = Source.c_str();
    const char* Removal;

    bool badchars = false;

    while (*Start)
    {
        badchars = false;

        Removal = Remove.c_str();

        while (*Removal)
        {
            if (*Removal == *Start)
            {
                badchars = true;
                break;
            }

            Removal++;
        }

        if (badchars)
        {
            Start++;
            continue;
        }

        Result.push_back(*Start);

        Start++;
    }

    return Result;
}

// makes a heap copy of a const string... remember to "delete []<string>" after use
char* copyStr(const char* SourceStr)
{
    int32_t len = strlen(SourceStr);

    char* NewStr = new char[len + 1];

    strcpy(NewStr, SourceStr);

    return NewStr;
}

void copyStr(char* dest, const char* source, int32_t maxLen)
{
    int32_t len = strlen(source);

    len = (len <= maxLen) ? len : maxLen;
    memcpy(dest, source, len);
    dest[len] = '\0';
}

// use on non cost data, modifies original string, make a copy
__strList splitStr(char* SourceStr, char* SplitChars)
{
    __strList Result = new std::vector<char*>();

    char* Start = SourceStr;
    char* Last = Start;
    char* Splits;

    while (*Start)
    {
        Splits = SplitChars;

        while (*Splits)
        {
            if (*Splits == *Start)
            {
                if (Start == SourceStr)
                {
                    Last++;
                }
                else
                {
                    *Start = 0;
                    Result->push_back(Last);
                    Last = Start + 1;
                }
                break;
            }

            Splits++;
        }

        Start++;
    }

    if (*Last)
        Result->push_back(Last);

    return Result;
}

// use on non cost data, modifies original string, make a copy
void splitStr(const std::string& SourceStr, std::string SplitChars, __stringList Result)
{
    char CopiedSource[8192];

    Result->clear();

    strncpy(CopiedSource, SourceStr.c_str(), 8191);
    //char* CopiedSource = copyStr( SourceStr.c_str() );

    char* Start = CopiedSource;
    char* Last = Start;
    const char* Splits;

    while (*Start)
    {
        Splits = SplitChars.c_str();

        while (*Splits)
        {
            if (*Splits == *Start)
            {
                if (Start == CopiedSource)
                {
                    Last++;
                }
                else
                {
                    *Start = 0;
                    Result->push_back(Last);
                    Last = Start + 1;
                }
                break;
            }

            Splits++;
        }

        Start++;
    }

    if (*Last)
        Result->push_back(Last);

    //delete []CopiedSource;
}

void toUpper(std::string& Text)
{
    std::transform(Text.begin(), Text.end(), Text.begin(), ::toupper);
}

void toLower(std::string& Text)
{
    std::transform(Text.begin(), Text.end(), Text.begin(), ::tolower);
}

std::string toLowerCase(std::string Text)
{
    std::transform(Text.begin(), Text.end(), Text.begin(), ::tolower);
    return Text;
}

void toLower(char* str)
{
    while (*str != 0)
    {
        (*str) = (char)tolower(*str);
        ++str;
    }
}

void split(const std::string& Source, char Token, std::vector<std::string>& Result)
{
    Result.clear();

    if (Source.size() == 0)
        return;

    int32_t Start = 0;
    int32_t End = 0;
    int32_t Size = Source.size();

    End = Source.find(Token, 0);

    while (End != -1)
    {
        // clears out multiple delimiters, like 1,2,,,,,,,3,4,5,6 so you don't end up with empties int the return array
        if (End - Start == 0)
        {
            while (Start != Size && Source[Start] == Token)
                Start++;

            End = Source.find(Token, Start);

            if (End == -1)
                break;
        }

        if (End - Start > 0)
            Result.push_back(Source.substr(Start, End - Start));

        Start += (End - Start) + 1;

        End = Source.find(Token, Start);
    }

    End = Size;

    if (End - Start > 0)
        Result.push_back(Source.substr(Start, End - Start));

    if (Result.size() == 0)
        Result.push_back(Source);
}

std::vector<std::string> split(const std::string& Source, char Token)
{
    std::vector<std::string> Result;

    split(Source, Token, Result);

    return Result;
}

std::string N2S(int32_t Value, int32_t MinWidth)
{
    char Buffer[256];
    sprintf(Buffer, "%i", Value);

    std::string result(Buffer);

    if (MinWidth)
    {
        while (result.length() < static_cast<size_t>(MinWidth))
            result = "0" + result;
    }

    return result;
};

std::string N2S(int64_t Value, int32_t MinWidth)
{
    char Buffer[256];
    sprintf(Buffer, INT64_FORMAT, Value);

    std::string result(Buffer);

    if (MinWidth)
    {
        while (result.length() < static_cast<size_t>(MinWidth))
            result = "0" + result;
    }

    return result;
};

void N2S(int64_t Value, std::string& Result)
{
    char Buffer[32];
    sprintf(Buffer, INT64_FORMAT, Value);
    Result = Buffer;
};

//based on javascript encodeURIComponent()
std::string char2hex(char dec)
{
    char dig1 = (dec & 0xF0) >> 4;
    char dig2 = (dec & 0x0F);
    if (0 <= dig1 && dig1 <= 9)
        dig1 += 48; //0,48inascii
    if (10 <= dig1 && dig1 <= 15)
        dig1 += 65 - 10; //a,97inascii
    if (0 <= dig2 && dig2 <= 9)
        dig2 += 48;
    if (10 <= dig2 && dig2 <= 15)
        dig2 += 65 - 10;

    std::string r;
    r.append(&dig1, 1);
    r.append(&dig2, 1);
    return r;
};
