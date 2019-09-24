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
            struct StackItem_s
            {
                std::string columnName;
                cvar value {NONE};
                int64_t hash {NONE};
                db::IndexBits bits;

                StackItem_s(db::IndexBits bits) :
                    bits(std::move(bits))
                {}

                StackItem_s(std::string columnName, cvar value, int64_t hash) :
                    columnName(std::move(columnName)),
                    value(std::move(value)),
                    hash(hash)
                {}
            };

            using Stack = std::vector<StackItem_s>;

		public:
			using IndexPair = std::tuple<std::string, openset::db::IndexBits, bool>;
			using IndexList = std::vector<IndexPair>;

            Stack stack;

			Macro_s macros;
			openset::db::Table* table;
			openset::db::TablePartitioned* parts;
			int partition;
			int stopBit;
			IndexList indexes;

			Indexing();
			~Indexing();

			void mount(
				openset::db::Table* tablePtr, 
				Macro_s& queryMacros, 
				int partitionNumber, 
				int stopAtBit);

            openset::db::IndexBits compositeBits(const db::Attributes::listMode_e mode);

			openset::db::IndexBits* getIndex(std::string name, bool &countable);

		private:
			openset::db::IndexBits buildIndex(HintOpList &index, bool countable);
		};
	};
};
