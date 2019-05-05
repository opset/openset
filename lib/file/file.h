#pragma once

#include "../include/libcommon.h"

#define FT_SHAREREAD    FILE_SHARE_READ;
#define FT_SHAREWRITE   FILE_SHARE_WRITE;
#define FT_SHARERW      ( FT_SHAREREAD | FT_SHAREWRITE )

namespace openset
{
	namespace IO
	{
		class File
		{
		public:
			static int64_t FileSize(const char* filename);
			static int64_t FileSize(std::string filename)
			{
				return FileSize(filename.c_str());
			}

			static bool FileExists(const char* filename);
			static bool FileExists(std::string filename)
			{
				return FileExists(filename.c_str());
			}

			static void FileDelete(const char* filename);
			static void FileDelete(std::string filename)
			{
				return FileDelete(filename.c_str());
			}

			static bool FileSetSize(const char* filename, int64_t size);
			static bool FileSetSize(std::string filename, int64_t size)
			{
				return FileSetSize(filename.c_str(), size);
			}

			static std::string load(std::string& namePath);

		private:
			File();
			~File();
			File(const File& file);
			File& operator=(const File& file);
		};

	}; // IO
}; // OpenSet
