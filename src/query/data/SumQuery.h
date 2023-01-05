//
// Created by camelboat on 18-11-1.
//

#ifndef PROJECT_SUMQUERY_H
#define PROJECT_SUMQUERY_H

#include "../Query.h"
#include <vector>

class SumQuery : public ConcurrentQuery {
    static constexpr const char *qname = "SUM";
    std::vector<std::pair<Table::FieldIndex, Table::ValueType> > field_sum;
public:
    using ConcurrentQuery::ConcurrentQuery;

    QueryResult::Ptr execute() override;

    QueryResult::Ptr mergeAndPrint() override;

    std::string toString() override;

    friend class SumTask;
};

class SumTask : public Task {
public:
    using Task::Task;
    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> local_sum;
    void execute() override;
};


#endif //PROJECT_SUMQUERY_H
