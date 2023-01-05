//
// Created by dominic on 18-10-29.
//

#include "DeleteQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <iostream>

constexpr const char *DeleteQuery::qname;

QueryResult::Ptr DeleteQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    if (!this->operands.empty()) {
        db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Operands Error."));
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Too many operands for delete"
        );
    }
    Table::SizeType counter = 0;
    try{
        if(db.table_locks.find(this->targetTable)==db.table_locks.end()){
            throw TableNameNotFound(
                    "Error accesing table \"" + this->targetTable + "\". Table not found."
            );
        }
        db.table_locks[this->targetTable]->lock();
        auto &table = db[this->targetTable];
        if(condition.empty()){
            counter = table.clear();
            db.addresult(this->id, make_unique<SuccessMsgResult>(qname, targetTable));
            db.table_locks[this->targetTable]->unlock();
            Query::Ptr tmp = db.queries_erase(this->id);
            return make_unique<RecordCountResult>(counter);
        }
        auto result = initCondition(table);

        if (result.second) {
            addTaskByPaging<DeleteTask>(table);
        }
        else{
            db.addresult(this->id,std::make_unique<RecordCountResult>(0));
            db.table_locks[this->targetTable]->unlock();
            Query::Ptr tmp = db.queries_erase(this->id);
        }
/*
        if (result.second) {
            for (auto it = table.begin(); it != table.end();) {
                if (this->evalCondition(*it)) {
                    it->deleteRow();
                    ++counter;
                } else
                    ++it;
            }
        }
*/
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"DELETE takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        //return make_unique<RecordCountResult>(counter);
        return make_unique<SuccessMsgResult>(qname);
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

std::string DeleteQuery::toString() {
    return "QUERY = DELETE " + this->targetTable + "\"";
}

QueryResult::Ptr DeleteQuery::mergeAndPrint() {
    Database &db = Database::getInstance();
    auto &table = db[this->targetTable];
    Table::SizeType counter = 0;
    std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
    ++complete_num;
    if(complete_num < (int)concurrency_num){
        return std::make_unique<NullQueryResult>();
    }
    for(const auto &task:subTasks){
        counter += task->getCounter();
    }
    table.updateByCache();
    db.addresult(this->id,std::make_unique<RecordCountResult>(counter));
    db.table_locks[this->targetTable]->unlock();
    Query::Ptr tmp = db.queries_erase(this->id);
    return std::make_unique<RecordCountResult>(counter);
}

void DeleteTask::execute() {
    using namespace std;
    auto real_query = dynamic_cast<DeleteQuery *>(query);
    //try{
        for (auto it = begin; it != end;++it) {
            if (real_query->evalCondition(*it)) {
                table->erase_marked(it);
                ++counter;
            } else{
                table->loadToCache(it);
            }
        }
        real_query->mergeAndPrint();
    //}
    /*
    catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(real_query->qname, table->name().c_str(), "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(real_query->qname, table->name().c_str(), e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(real_query->qname, table->name().c_str(), "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(real_query->qname, table->name().c_str(), "Unkonwn error '?'."_f % e.what());
    }
*/
}
