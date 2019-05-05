#pragma once

#include "../include/libcommon.h"
#include <vector>

#ifdef _MSC_VER
#include <direct.h>
#define GetCurrentDir _getcwd
#define DIR_SEPARATOR '/' // note in the SDK a forward slash is valid on win32.
#else
#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>
#define GetCurrentDir getcwd
#define DIR_SEPARATOR '/'
#endif

namespace openset
{
	namespace IO
	{
		class Directory
		{
		public:
			Directory();
			~Directory();

			static std::string GetCurrentExecutionDirectory();
			static bool mkdir(std::string path)
			{
#ifdef _MSC_VER
				return (_mkdir(path.c_str()) == 0);
#else
				return (::mkdir(path.c_str(), ACCESSPERMS) == 0);
#endif
			}

			bool Open(std::string& mask);
			void Close();
			std::string GetDirectory();
			bool FirstFile(std::string& filename);
			bool NextFile(std::string& filename);

		private:
			std::string _Directory;
			std::string _DirectoryMask;
			std::vector<std::string> _Files;
			size_t _Index;

			Directory(const Directory& dir);
			Directory& operator=(const Directory& dir);
		};
	}; // IO
}; // OpenSet
