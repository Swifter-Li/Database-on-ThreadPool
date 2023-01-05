//
// Created by liu on 18-10-25.
//

#include "DumpTableQuery.h"
#include "../../db/Database.h"

#include <fstream>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *DumpTableQuery::qname;

QueryResult::Ptr DumpTableQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    auto &db = Database::getInstance();
    try {
        if(db.table_locks.find(this->targetTable)==db.table_locks.end()){

            db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Table Missing."));
            Query::Ptr tmp = db.queries_erase(this->id);
            throw TableNameNotFound(
                    "Error accesing table \"" + this->targetTable + "\". Table not found."
            );
        }
        db.table_locks[this->targetTable]->lock();
        ofstream outfile(this->fileName);
        if (!outfile.is_open()) {

            db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Not File."));
            Query::Ptr tmp = db.queries_erase(this->id);
            return make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
        }
        outfile << db[this->targetTable];
        outfile.close();
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"DUMP takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        db.addresult(this->id,make_unique<SuccessMsgResult>(qname, targetTable));
        db.table_locks[this->targetTable]->unlock();
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<SuccessMsgResult>(qname, targetTable);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string DumpTableQuery::toString() {
    return "QUERY = Dump TABLE, FILE = \"" + this->fileName + "\"";
}
