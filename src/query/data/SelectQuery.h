//
// Created by dominic on 18-10-29.
//

#ifndef PROJECT_SELECTQUERY_H
#define PROJECT_SELECTQUERY_H

#include "../Query.h"

class SelectQuery : public ConcurrentQuery {
    static constexpr const char *qname = "SELECT";
public:
    using ConcurrentQuery::ConcurrentQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    QueryResult::Ptr mergeAndPrint() override;

    //std::mutex answerLock;

    std::map<Table::KeyType, std::vector<Table::ValueType *> > selectAnswer;

    friend class SelectTask;
};

class SelectTask : public Task {
    using Task::Task;

    void execute() override;

public:
    std::map<Table::KeyType, std::vector<Table::ValueType *> > localAnswer;
};


#endif //PROJECT_SELECTQUERY_H
