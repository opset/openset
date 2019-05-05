#include "file.h"

#include "directory.h"

#ifdef _MSC_VER
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#endif

namespace openset
{
	namespace IO
	{
		int64_t File::FileSize(const char* filename)
		{
			int64_t filesize = 0;

			FILE* file = fopen(filename, "r");

			if (file)
			{
				fseek(file, 0L, SEEK_END);
				filesize = ftell(file);

				fclose(file);
			}

			return filesize;
		}

		bool File::FileExists(const char* filename)
		{
			FILE* file = fopen(filename, "r");
			bool result = (file != NULL);

			if (file)
				fclose(file);

			return result;
		}

		void File::FileDelete(const char* filename)
		{
			remove(filename);
		}

		bool File::FileSetSize(const char* filename, int64_t size)
		{
#ifdef _MSC_VER
			HANDLE Handle;
#else
            int32_t      Handle;
#endif

#ifdef _MSC_VER

			Handle = CreateFile(
				filename,
				GENERIC_READ | GENERIC_WRITE,
				FILE_SHARE_READ | FILE_SHARE_WRITE,
				NULL,
				OPEN_ALWAYS,
				FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING | FILE_FLAG_RANDOM_ACCESS,
				NULL);

			if (Handle == INVALID_HANDLE_VALUE)
				return false;

#else

    Handle = open( filename, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );

    if ( Handle == -1 )
        return false;

#endif

#ifdef _MSC_VER

			DWORD high = ((DWORD*)&size)[1];
			DWORD low = ((DWORD*)&size)[0];

			if (SetFilePointer(Handle, low, (long*)&high, FILE_BEGIN) == INVALID_SET_FILE_POINTER)
				return false;

			SetEndOfFile(Handle);

#else

    if ( lseek( Handle, size - 1, SEEK_SET ) == -1 )
        return false;

    if ( write( Handle, "", 1 ) != 1 )
        return false;

#endif

#ifdef _MSC_VER

			CloseHandle(Handle);

#else

    close( Handle );

#endif

			return true;
		}

		std::string File::load(std::string& namePath)
		{
			auto size = FileSize(namePath.c_str());
			auto file = fopen(namePath.c_str(), "r");

			if (file == nullptr)
				return "";

			auto data = new char[size + 1];

			fread(data, 1, size, file);
			fclose(file);

			data[size] = 0;

			auto content = std::string(data);
			delete[]data;

			return content;
		}
	}; // IO
}; // OpenSet
