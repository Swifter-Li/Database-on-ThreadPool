//
// Created by liu on 18-10-25.
//

#include "DropTableQuery.h"
#include "../../db/Database.h"

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *DropTableQuery::qname;

QueryResult::Ptr DropTableQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    try {
        if(db.table_locks.find(this->targetTable)==db.table_locks.end()){

            db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Table Missing."));
            Query::Ptr tmp = db.queries_erase(this->id);
            throw TableNameNotFound(
                    "Error accesing table \"" + this->targetTable + "\". Table not found."
            );
        }
        db.table_locks[this->targetTable]->lock();
        db.dropTable(this->targetTable);
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"DROP takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        db.addresult(this->id,make_unique<SuccessMsgResult>(qname));
        db.table_locks[this->targetTable]->unlock();
        db.table_locks.erase(this->targetTable);
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<SuccessMsgResult>(qname);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, targetTable, "No such table."s);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string DropTableQuery::toString() {
    return "QUERY = DROP, Table = \"" + targetTable + "\"";
}