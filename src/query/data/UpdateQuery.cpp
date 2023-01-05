//
// Created by liu on 18-10-25.
//

#include "UpdateQuery.h"
#include "../../db/Database.h"

#include <iostream>
#ifdef TIMER
#include <iostream>

#endif

constexpr const char *UpdateQuery::qname;

QueryResult::Ptr UpdateQuery::execute() {

    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    if (this->operands.size() != 2) {

        db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Operands Error."));
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    }
    Table::SizeType counter = 0;
    try {
        if(db.table_locks.find(this->targetTable)==db.table_locks.end()){
            throw TableNameNotFound(
                    "Error accesing table \"" + this->targetTable + "\". Table not found."
            );
        }
        //std::cerr << "sub before lock\n";
        db.table_locks[this->targetTable]->lock();
        //std::cerr << "sub after lock\n";
        auto &table = db[this->targetTable];
        if (this->operands[0] == "KEY") {
            this->keyValue = this->operands[1];
        } else {
            this->fieldId = table.getFieldIndex(this->operands[0]);
            this->fieldValue = (Table::ValueType) strtol(this->operands[1].c_str(), nullptr, 10);
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    if (this->keyValue.empty()) {
                        (*it)[this->fieldId] = this->fieldValue;
                    } else {
                        it->setKey(this->keyValue);
                    }
                    ++counter;
                }
            }
        }
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"UPDATE takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        db.addresult(this->id,make_unique<RecordCountResult>(counter));
        db.table_locks[this->targetTable]->unlock();
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<RecordCountResult>(counter);
    }
    catch (const TableNameNotFound &e) {
        db.addresult(this->id, make_unique<SuccessMsgResult>(qname, targetTable));
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        db.addresult(this->id, make_unique<SuccessMsgResult>(qname, targetTable));
        db.table_locks[this->targetTable]->unlock();
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        db.addresult(this->id, make_unique<SuccessMsgResult>(qname, targetTable));
        db.table_locks[this->targetTable]->unlock();
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        db.addresult(this->id, make_unique<SuccessMsgResult>(qname, targetTable));
        db.table_locks[this->targetTable]->unlock();
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string UpdateQuery::toString() {
    return "QUERY = UPDATE " + this->targetTable + "\"";
}
