#pragma once

#include "../include/libcommon.h"

class Path
{
public:
	static bool IsRelative(const std::string& path);
	static std::string GetExtension(const std::string& path);
};

