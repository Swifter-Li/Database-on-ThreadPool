//
// Created by Reapor Yurnero on 31/10/2018.
//

#ifndef PROJECT_SWAPQUERY_H
#define PROJECT_SWAPQUERY_H

#include "../Query.h"

class SwapQuery : public ComplexQuery {
    static constexpr const char *qname = "SWAP";
    Table::FieldIndex swapA, swapB;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    //bool modify() override { return true; }
};

#endif //PROJECT_SWAPQUERY_H
