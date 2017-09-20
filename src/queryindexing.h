#pragma once

#include "querycommon.h"
#include "columns.h"
#include "indexbits.h"
#include "table.h"
#include <stack>

namespace openset
{
	namespace query
	{
		class Indexing
		{
		public:
			using IndexPair = std::tuple<std::string, openset::db::IndexBits, bool>;
			using IndexList = std::vector<IndexPair>;

			macro_s macros;
			openset::db::Table* table;
			openset::db::TablePartitioned* parts;
			int partition;
			int stopBit;
			IndexList indexes;

			Indexing();
			~Indexing();

			void mount(
				openset::db::Table* tablePtr, 
				macro_s& queryMacros, 
				int partitionNumber, 
				int stopAtBit);

			openset::db::IndexBits* getIndex(std::string name, bool &countable);

		private:
			openset::db::IndexBits buildIndex(HintOpList &index, bool &countable);
		};
	};
};
