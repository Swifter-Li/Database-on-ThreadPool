//
// Created by liu on 18-10-25.
//

#include "LoadTableQuery.h"
#include "../../db/Database.h"

#include <fstream>
#include <iostream>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *LoadTableQuery::qname;

QueryResult::Ptr LoadTableQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    try {
        /*
        ifstream infile(this->fileName);
        if (!infile.is_open()) {

            db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Not File."));
            Query::Ptr tmp = db.queries_erase(this->id);
            return make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
        }
         */
        this->tablename = db.getFileTableName(this->fileName);
        db.add_table_lock(this->tablename);
        db.table_locks[tablename]->lock();
        //infile.close();
        addspecialTask<LoadTask>(db);

#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"LOAD takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<SuccessMsgResult>(qname, targetTable);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

template<class RealTask>
void LoadTableQuery::addspecialTask(Database &db, Table *table) {
    ThreadPool &threadPool = ThreadPool::getPool();
    auto newTask = std::unique_ptr<RealTask>(new RealTask(this));
    auto newTaskPtr = newTask.get();
    subTasks.emplace_back(std::move(newTask));
    threadPool.addTask(newTaskPtr);
}

std::string LoadTableQuery::toString() {
    return "QUERY = Load TABLE, FILE = \"" + this->fileName + "\"";
}

void LoadTableQuery::addresult_to_db() {
    Database &db=Database::getInstance();
    db.table_locks[this->tablename]->unlock();

    db.addresult(this->id,std::make_unique<SuccessMsgResult>(qname, targetTable));
    Query::Ptr tmp = db.queries_erase(this->id);
}

void LoadTask::execute() {
    Database &db = Database::getInstance();
    auto real_query = dynamic_cast<LoadTableQuery *>(query);
    std::ifstream infile(real_query->fileName);
    db.loadTableContentFromStream(infile, real_query->fileName);
    infile.close();
    real_query->addresult_to_db();
}
