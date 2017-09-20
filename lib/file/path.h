#ifndef RARELOGIC_CPPLIP_FILE_PATH_H_
#define RARELOGIC_CPPLIP_FILE_PATH_H_

#include "../include/libcommon.h"

class Path
{
public:
	static bool IsRelative(const std::string& path);
	static std::string GetExtension(const std::string& path);
};

#endif // RARELOGIC_CPPLIP_FILE_PATH_H_
