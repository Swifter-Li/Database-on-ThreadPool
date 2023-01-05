//
// Created by liu on 18-10-25.
//

#include "ListTableQuery.h"
#include "../../db/Database.h"

constexpr const char *ListTableQuery::qname;

QueryResult::Ptr ListTableQuery::execute() {

    Database &db = Database::getInstance();
    if(db.table_locks.find(this->targetTable)==db.table_locks.end()){

        db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Table Missing."));
        Query::Ptr tmp = db.queries_erase(this->id);
        throw TableNameNotFound(
                "Error accesing table \"" + this->targetTable + "\". Table not found."
        );
    }
    db.table_locks[this->targetTable]->lock();
    db.printAllTable();
    db.addresult(this->id,std::make_unique<SuccessMsgResult>(qname));
    db.table_locks[this->targetTable]->unlock();
    Query::Ptr tmp = db.queries_erase(this->id);
    return std::make_unique<SuccessMsgResult>(qname);
}

std::string ListTableQuery::toString() {
    return "QUERY = LIST";
}

