//
// Created by dominic on 18-11-17.
//

#ifndef PROJECT_TASK_H
#define PROJECT_TASK_H

#include "../query/Query_Base.h"
#include "../db/Table.h"

class Task {
public:
//protected:
    Query *query;
    Table::Iterator begin;
    Table::Iterator end;
    Table *table;
    Table::SizeType counter;

public:
    explicit Task(Query *query, Table::Iterator begin, Table::Iterator end, Table *table = nullptr);

    explicit Task(Query *query);

    Task(const Task &) = delete;

    Task(Task &&) = delete;

    Task &operator=(const Task &) = delete;

    Task &operator=(Task &&) = delete;

    virtual ~Task() = default;

    virtual void execute();

    Table::SizeType getCounter();

};


#endif //PROJECT_TASK_H
