//
// Created by Reapor Yurnero on 31/10/2018.
//

#ifndef PROJECT_SUBQUERY_H
#define PROJECT_SUBQUERY_H

#include "../Query.h"


class SubQuery : public ConcurrentQuery {
    static constexpr const char *qname = "SUB";
#ifdef TIMER
    struct timespec ts1, ts2;
#endif

public:
    using ConcurrentQuery::ConcurrentQuery;

    std::vector<Table::FieldIndex > sub_src;
    Table::FieldIndex sub_victim, sub_des;
    Table::SizeType counter=0;

    std::mutex g_mutex;
    std::mutex write_mutex;

    QueryResult::Ptr execute() override;

    QueryResult::Ptr mergeAndPrint() override;

    std::string toString() override;

    //bool modify() override { return false; }
};

class SubTask : public Task {
public:
    using Task::Task;
    Table::SizeType local_counter=0;
    void execute() override;
};

#endif //PROJECT_SUBQUERY_H
