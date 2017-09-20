#include "path.h"

bool Path::IsRelative(const std::string& path)
{
	if (path.find("..\\") != -1)
	{
		return true;
	}

	if (path.find("../") != -1)
	{
		return true;
	}

	return false;
}

std::string Path::GetExtension(const std::string& path)
{
	int32_t pos = path.find_last_of('.');

	if (pos == -1)
	{
		return "";
	}

	// if the last character is the period then return
	if (pos + 1 >= path.size())
	{
		return "";
	}

	return path.substr(pos + 1);
}
