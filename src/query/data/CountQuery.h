//
// Created by dominic on 18-10-29.
//

#ifndef PROJECT_COUNTQUERY_H
#define PROJECT_COUNTQUERY_H

#include "../Query.h"

class CountQuery : public ConcurrentQuery {
    static constexpr const char *qname = "COUNT";
public:
    using ConcurrentQuery::ConcurrentQuery;

    unsigned int countresult = 0;

    std::mutex count_mutex;

    QueryResult::Ptr execute() override;

    QueryResult::Ptr mergeAndPrint() override;

    std::string toString() override;

    //bool modify() override { return false; }
};

class CountTask : public Task {
public:
    using Task::Task;
    void execute() override;

    unsigned int local_count = 0;
};


#endif //PROJECT_COUNTQUERY_H
