//
// Created by dominic on 18-10-29.
//

#include "CountQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *CountQuery::qname;

QueryResult::Ptr CountQuery::execute() {
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
                "Too many operands for count"
        );

    }
    try{
        if(db.table_locks.find(this->targetTable)==db.table_locks.end()){
            throw TableNameNotFound(
                    "Error accesing table \"" + this->targetTable + "\". Table not found."
            );
        }
        db.table_locks[this->targetTable]->lock();
        auto &table = db[this->targetTable];

        auto result = initCondition(table);
        if (result.second) {
//            for (auto it = table.begin(); it != table.end();++it) {
//                if (this->evalCondition(*it))
//                    ++counter;
//            }
            addTaskByPaging<CountTask>(table);
        }
        else{
            db.addresult(this->id,std::make_unique<AnswerMsgResult>(0));
            db.table_locks[this->targetTable]->unlock();
            Query::Ptr tmp = db.queries_erase(this->id);
        }
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"COUNT takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
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

std::string CountQuery::toString() {
    return "QUERY = Count " + this->targetTable + "\"";
}

QueryResult::Ptr CountQuery::mergeAndPrint() {
    Database &db = Database::getInstance();
    std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
    ++complete_num;
    if(complete_num < (int)concurrency_num){
        return std::make_unique<NullQueryResult>();
    }
    for(const auto &task:subTasks){
        auto real_task = dynamic_cast<CountTask *>(task.get());
        countresult += real_task->local_count;
    }
    db.addresult(this->id,std::make_unique<AnswerMsgResult>(this->countresult));
    db.table_locks[this->targetTable]->unlock();
    Query::Ptr tmp = db.queries_erase(this->id);
    return std::make_unique<NullQueryResult>();
}

void CountTask::execute() {
    auto real_query = dynamic_cast<CountQuery *>(query);
    //unsigned int local_count = 0;
    for (auto table_it = begin;table_it != end;++table_it) {
        if (real_query->evalCondition(*table_it)) ++local_count;
    }
    //update the global count result
    /*
    {
        std::unique_lock<std::mutex> lock(real_query->count_mutex);
        real_query->countresult += local_count;
    }
     */
    real_query->mergeAndPrint();
}