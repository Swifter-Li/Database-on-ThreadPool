//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "MinQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

// #define TIMER
#ifdef TIMER
#include <iostream>

#endif

#include <algorithm>

constexpr const char *MinQuery::qname;

QueryResult::Ptr MinQuery::execute() {
    using namespace std;
#ifdef TIMER
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    if (this->operands.empty()) {

        db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Operands Error."));
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "No operand (? operands)."_f % operands.size()
        );
    }
    try {
        /*this->max.clear();*/
        if(db.table_locks.find(this->targetTable)==db.table_locks.end()){
            throw TableNameNotFound(
                    "Error accesing table \"" + this->targetTable + "\". Table not found."
            );
        }
        db.table_locks[this->targetTable]->lock();

        this->min.reserve(this->operands.size());
        auto &table = db[this->targetTable];

        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for MAX.)"_f
                );
            } else {
                this->min.emplace_back(make_pair(table.getFieldIndex(*it),Table::ValueTypeMax));
            }
        }
        auto result = initCondition(table);
        if (result.second) {
            addTaskByPaging<MinTask>(table);
        }
        else{
            db.addresult(this->id,std::make_unique<NullQueryResult>());
            db.table_locks[this->targetTable]->unlock();
            Query::Ptr tmp = db.queries_erase(this->id);
        }
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

void MinTask::execute() {
    auto real_query = dynamic_cast<MinQuery *>(query);
    local_min = real_query->min;
    for (auto table_it = begin;table_it != end;++table_it) {
        if (real_query->evalCondition(*table_it)) {
            findsomething |= true;
            for (auto min_it=local_min.begin();min_it!=local_min.end();++min_it) {
                if ( (*table_it)[(*min_it).first] < (*min_it).second )
                    (*min_it).second = (*table_it)[(*min_it).first];
            }
        }
    }
    //update the global max
    /*
    {
        std::unique_lock<std::mutex> lock(real_query->g_mutex);
        for (size_t i=0;i<local_min.size();i++){
            if(local_min[i].second < real_query->min[i].second)
                std::swap(local_min[i].second, real_query->min[i].second);
        }
    }
     */
    real_query->mergeAndPrint();
}

QueryResult::Ptr MinQuery::mergeAndPrint() {
    Database &db = Database::getInstance();
    //auto &table = db[this->targetTable];
    std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
    ++complete_num;
    if(complete_num < (int)concurrency_num){
        return std::make_unique<NullQueryResult>();
    }
    bool queryresultnonempty = false;
    for(const auto &task:subTasks){
        auto real_task = dynamic_cast<MinTask *>(task.get());
        queryresultnonempty |= real_task->findsomething;
        for (size_t i=0;i<real_task->local_min.size();i++){
            if(real_task->local_min[i].second < min[i].second)
                std::swap(real_task->local_min[i].second, min[i].second);
        }
    }
    if (queryresultnonempty) {
        std::vector<Table::ValueType> min_result;
        for (unsigned int i=0;i<this->min.size();i++){
            min_result.emplace_back(this->min.at(i).second);
        }
        db.addresult(this->id,std::make_unique<AnswerMsgResult>(std::move(min_result)));
    }
    else {
        db.addresult(this->id,std::make_unique<SuccessMsgResult>(qname));
    }
    db.table_locks[this->targetTable]->unlock();
    Query::Ptr tmp = db.queries_erase(this->id);
    //allow the next query to go
    //std::cout<<"table lock released\n";
#ifdef TIMER
    clock_gettime(CLOCK_MONOTONIC, &ts2);
    std::cerr<<"MAX takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
    return std::make_unique<NullQueryResult>();
}

std::string MinQuery::toString() {
    return "QUERY = MIN " + this->targetTable + "\"";
}