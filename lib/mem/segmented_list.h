#pragma once
#include <vector>
#include "../sba/sba.h"

template <typename tEntry, int64_t elements>
class SegmentedList
{
    struct PageStruct_s
    {
        tEntry values[elements - 1];
    };

    int64_t elementsPerPage {elements - 1};

    using Pages = std::vector<PageStruct_s*>;

    Pages pages;
    int64_t listSize{0};
public:
    SegmentedList() = default;
    ~SegmentedList()
    {
        for (auto page: pages)
            PoolMem::getPool().freePtr(page);

        pages.clear();
    }

    tEntry& at(int64_t index)
    {
        if (index < 0 || index > listSize)
            throw std::runtime_error("segmented_list index out of range");
        return pages.at(index / elementsPerPage)->values[index % elementsPerPage];
    }

    void push_back(tEntry entry)
    {
        if (listSize / elementsPerPage == pages.size())
            pages.push_back(reinterpret_cast<PageStruct_s*>(PoolMem::getPool().getPtr(sizeof(PageStruct_s))));
        pages.at(listSize / elementsPerPage)->values[listSize % elementsPerPage] = entry;
        ++listSize;
    }

    int64_t size() const
    {
        return listSize;
    }

private:
    PageStruct_s* getPage(int64_t index)
    {
        index /= elementsPerPage;

        while (index >= static_cast<int64_t>(pages.size()))
        {
            const auto page = reinterpret_cast<PageStruct_s*>(PoolMem::getPool().getPtr(sizeof(PageStruct_s)));
            pages.push_back(page);
        }

        return pages.at(index);
    }


};