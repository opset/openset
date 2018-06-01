#pragma once

#include "grid.h"
#include "attributes.h"
#include "attributeblob.h"

using namespace std;

namespace openset
{
	namespace db
	{
		// forward references
		class Table;
		class Attributes;
		class AttributeBlob;
		class People;

		/*! \class Person
		 *
		 *  Reusable Container for managing personData_s structures
		 *  
		 *  The idea is that for an insert job or query job
		 *  a person object would be created, mapped to the
		 *  correct table (and as such, the schema and 
		 *  partition) then re-used by calling mount with
		 *  different raw personData_s pointers. This allows
		 *  for the expensive configuration to be done once
		 *  per job.
		 *  
		 *  The usage is as follows:
		 *  
		 *  1. call mapTable 
		 *  2. call either mapSchema 
		 *     - without params to map all columns to the grid
		 *     - with a column list to map specific columns (for query)
		 *  3. call prepare to map customer data to Grid object
		 *  4. do work. This could be insert, and commit, or just reading
		 */
		class Person
		{

		private:
			Grid grid;
			Table* table;
			Attributes* attributes;
			AttributeBlob* blob;
			People* people;
			int partition;

		public:
			Person();
			~Person() = default;

			// totally reset the person object back to square one
			void reinit();

			/**
			 * \brief map a table and partition to this Person object
			 * \param[in] tablePtr pointer to a Table object
			 * \param[in] Partition number this object lives in
			 */
			bool mapTable(Table* tablePtr, int Partition);
			bool mapTable(Table* tablePTr, int Partition, vector<string>& columnNames);

			/**
			 * \brief maps a personData_s object to the Person object
			 * \param[in] personData 
			 */
			void mount(PersonData_s* personData);

			/**
			 * \brief expands personData_s object into Grid object
			 */
			void prepare();

			void setSessionTime(const int64_t sessionTime)
			{
				grid.setSessionTime(sessionTime);
			}

			/**
			 * \brief return reference to grid object
			 * \return Grid const pointer (read only)
			 */
			inline Grid* getGrid() 
			{
				return &grid;
			}

			int64_t getUUID() const
			{
				return grid.getMeta()->id;
			}

			inline PersonData_s* getMeta() const
			{
				return grid.getMeta();
			}

			/**
			 * \brief insert a single JSON row into the Person.grid object
			 * \param rowData single row JSON document object.
			 */
			void insert(cjson* rowData);

			/**
			 * \brief commit (re-compress) the data in Person.grid
			 * 
			 * \remarks this will rebuild a new personData_s structure and update
			 *          the Table.people.linearIndex to reflect the change.
			 *          
			 * \note The personData_s pointer passed to mount
			 *       from the caller will be invalid, so this commit
			 *       returns the new pointer if this is important.
			 */
			PersonData_s* commit();

		private:
			/**
			* map the entire schema to the Person.grid object, called by
			* map table
			* \return
			*/
			bool mapSchemaAll();

			/**
			* map a portion of the schema to the Person.grid object, this is
			* used during a query, and is called by mapTable
			* 
			* \param[in] columnNames list of columns we want to extract
			* \return success
			*/
			bool mapSchemaList(const vector<string>& columnNames);

		};
	};
};
