#include "directory.h"

#include <algorithm>
//#include <error.h>

#include "../str/strtools.h"

namespace openset
{
	namespace IO
	{
		Directory::Directory()
			: _Directory(),
			_DirectoryMask(),
			_Files(),
			_Index(0)
		{}

		Directory::~Directory() = default;

		/*static*/
		std::string Directory::GetCurrentExecutionDirectory()
		{
			char buffer[FILENAME_MAX];

			if (GetCurrentDir(buffer, sizeof(buffer)))
			{
				buffer[FILENAME_MAX - 1] = '\0';
				return std::string(buffer) + DIR_SEPARATOR;
			}
			else
			{
				return "";
			}
		}

		bool Directory::Open(std::string& mask)
		{
			size_t idx = mask.find_last_of(DIR_SEPARATOR);

			this->_Files.clear();

			if (idx == std::string::npos)
				return false;

			this->_Directory = mask.substr(0, idx + 1);
			this->_DirectoryMask = mask;

#ifdef _MSC_VER

			WIN32_FIND_DATAA fileInfo;
			HANDLE file;

			file = FindFirstFile(_DirectoryMask.c_str(), &fileInfo);

			if (file == INVALID_HANDLE_VALUE)
				return false;

			while (file != INVALID_HANDLE_VALUE)
			{
				if ((strcmp(fileInfo.cFileName, ".") != 0) &&
					(strcmp(fileInfo.cFileName, "..") != 0))
				{
					std::string filename = fileInfo.cFileName;

					toLower(filename);

					this->_Files.push_back(filename);
				};

				if (!FindNextFile(file, &fileInfo))
					break;
			};

			FindClose(file);

#else
    
    struct dirent* dp;
    DIR* dir = opendir( this->_Directory.c_str() );

    if ( dir == NULL )
        return false;

    while ( ( dp = readdir( dir ) ) != NULL )
    {
        if ( ( strcmp( dp->d_name, "." ) != 0 ) &&
             ( strcmp( dp->d_name, ".." ) != 0 ) )
            this->_Files.push_back( std::string( dp->d_name ) );
    }

    closedir( dir );

#endif

			std::sort(_Files.begin(), _Files.end());

			this->_Index = 0;

			return true;
		}

		void Directory::Close()
		{
			this->_Files.clear();
		}

		std::string Directory::GetDirectory()
		{
			return this->_Directory;
		}

		bool Directory::FirstFile(std::string& filename)
		{
			this->_Index = 0;

			if (this->_Index >= this->_Files.size())
			{
				filename = "";
				return false;
			}

			return this->NextFile(filename);
		}

		bool Directory::NextFile(std::string& filename)
		{
			if (this->_Index >= this->_Files.size())
			{
				filename = "";
				return false;
			}

			filename = this->_Files[this->_Index];

			++(this->_Index);

			return true;
		}
	}; // IO
}; // OpenSet
