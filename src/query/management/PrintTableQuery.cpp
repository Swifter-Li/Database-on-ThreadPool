//
// Created by liu on 18-10-25.
//

#include "PrintTableQuery.h"
#include "../../db/Database.h"

#include <iostream>

constexpr const char *PrintTableQuery::qname;

QueryResult::Ptr PrintTableQuery::execute() {
    using namespace std;
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
        auto &table = db[this->targetTable];
        cout << "================\n";
        cout << "TABLE = ";
        cout << table;
        cout << "================\n" << endl;
        db.addresult(this->id,make_unique<SuccessMsgResult>(qname, this->targetTable));
        db.table_locks[this->targetTable]->unlock();
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<SuccessMsgResult>(qname, this->targetTable);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    }
}

std::string PrintTableQuery::toString() {
    return "QUERY = SHOWTABLE, Table = \"" + this->targetTable + "\"";
}