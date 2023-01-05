//
// Created by dominic on 18-10-29.
//

#ifndef PROJECT_DELETEQUERY_H
#define PROJECT_DELETEQUERY_H

#include "../Query.h"


class DeleteQuery : public ConcurrentQuery {
    static constexpr const char *qname = "DELETE";
public:
    using ConcurrentQuery::ConcurrentQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    QueryResult::Ptr mergeAndPrint() override;

    friend class DeleteTask;
};

class DeleteTask : public Task {
public:
    using Task::Task;

    void execute() override;
};

#endif //PROJECT_DELETEQUERY_H
