//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_LOADTABLEQUERY_H
#define PROJECT_LOADTABLEQUERY_H


#include "../Query.h"



class LoadTableQuery : public Query {
    static constexpr const char *qname = "LOAD";

    std::vector<std::unique_ptr<Task> > subTasks;

#ifdef TIMER
    struct timespec ts1, ts2;
#endif

public:
    const std::string fileName;

    std::string tablename;

    explicit LoadTableQuery(std::string table, std::string fileName) :
            Query(std::move(table)), fileName(std::move(fileName)) {}

    QueryResult::Ptr execute() override;

    std::string toString() override;

    //bool modify() override { return false; }
    template<class RealTask>
    void addspecialTask(Database &db, Table *table = nullptr);

    void addresult_to_db();
};

class LoadTask : public Task {
public:
    using Task::Task;
    void execute() override;
};

#endif //PROJECT_LOADTABLEQUERY_H
