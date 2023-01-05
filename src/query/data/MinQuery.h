//
// Created by Reapor Yurnero on 31/10/2018.
//

#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include "../Query.h"

class MinQuery : public ConcurrentQuery {
    static constexpr const char *qname = "MIN";
#ifdef TIMER
    struct timespec ts1, ts2;
#endif

public:
    using ConcurrentQuery::ConcurrentQuery;

    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> min;

    std::mutex g_mutex;

    QueryResult::Ptr execute() override;

    QueryResult::Ptr mergeAndPrint() override;

    std::string toString() override;

    //bool modify() override { return false; }
};

class MinTask : public Task {
public:
    using Task::Task;
    bool findsomething = false;
    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> local_min;
    void execute() override;
};
#endif //PROJECT_MINQUERY_H
