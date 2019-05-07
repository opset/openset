#include "path.h"

bool Path::IsRelative(const std::string& path)
{
	if (path.find("..\\") != std::string::npos)
	{
		return true;
	}

	if (path.find("../") != std::string::npos)
	{
		return true;
	}

	return false;
}

std::string Path::GetExtension(const std::string& path)
{
    const auto pos = path.find_last_of('.');

	if (pos == std::string::npos)
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
