//
// Created by wenda on 10/30/18.
//

#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include "../Query.h"
//#define TIMER

class MaxQuery : public ConcurrentQuery {
    static constexpr const char *qname = "MAX";
#ifdef TIMER
    struct timespec ts1, ts2;
#endif

public:
    using ConcurrentQuery::ConcurrentQuery;

    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> max;

    //std::mutex g_mutex;

    QueryResult::Ptr execute() override;

    QueryResult::Ptr mergeAndPrint() override;

    std::string toString() override;

    //bool modify() override { return false; }
};

class MaxTask : public Task {
public:
    using Task::Task;
    bool findsomething = false;
    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> local_max;
    void execute() override;
};

#endif //PROJECT_MAXQUERY_H
