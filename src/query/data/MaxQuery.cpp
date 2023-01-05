//
// Created by wenda on 10/30/18.
//

#include "MaxQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

// open counter for MAX solely

#include <iostream>

#include <unistd.h>
constexpr const char *MaxQuery::qname;


QueryResult::Ptr MaxQuery::execute() {
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

        this->max.reserve(this->operands.size());
        auto &table = db[this->targetTable];

        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for MAX.)"_f
                );
            } else {
                this->max.emplace_back(make_pair(table.getFieldIndex(*it),Table::ValueTypeMin));
            }
        }
        auto result = initCondition(table);

        /*
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    for (auto iter=this->max.begin();iter!=this->max.end();++iter){
                        if( (*it)[(*iter).first] > (*iter).second )
                            (*iter).second = (*it)[(*iter).first];
                    }
                }
            }
        }

        vector<Table::ValueType> max_result;
        for (unsigned int i=0;i<this->max.size();i++){
            max_result.emplace_back(this->max.at(i).second);
        }*/
        if (result.second) {
            addTaskByPaging<MaxTask>(table);
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

std::string MaxQuery::toString() {
    return "QUERY = MAX " + this->targetTable + "\"";
}

void MaxTask::execute() {
    auto real_query = dynamic_cast<MaxQuery *>(query);
    local_max = real_query->max;
    for (auto table_it = begin;table_it != end;++table_it) {
        if (real_query->evalCondition(*table_it)) {
            findsomething |= true;
            for (auto max_it=local_max.begin();max_it!=local_max.end();++max_it) {
                if ( (*table_it)[(*max_it).first] > (*max_it).second )
                    (*max_it).second = (*table_it)[(*max_it).first];
            }
        }
    }
    //update the global max
    /*
    {
        std::unique_lock<std::mutex> lock(real_query->g_mutex);
        for (size_t i=0;i<local_max.size();i++){
            if(local_max[i].second > real_query->max[i].second)
                std::swap(local_max[i].second, real_query->max[i].second);
        }
    }
     */
    real_query->mergeAndPrint();
}

QueryResult::Ptr MaxQuery::mergeAndPrint() {
    Database &db = Database::getInstance();
    //auto &table = db[this->targetTable];
    std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
    ++complete_num;
    if(complete_num < (int)concurrency_num){
        return std::make_unique<NullQueryResult>();
    }
    bool queryresultnonempty = false;
    for(const auto &task:subTasks){
        auto real_task = dynamic_cast<MaxTask *>(task.get());
        queryresultnonempty |= real_task->findsomething;
        for (size_t i=0;i<real_task->local_max.size();i++){
            if(real_task->local_max[i].second > max[i].second)
                std::swap(real_task->local_max[i].second, max[i].second);
        }
    }
    if (queryresultnonempty) {
        std::vector<Table::ValueType> max_result;
        for (unsigned int i=0;i<this->max.size();i++){
            max_result.emplace_back(this->max.at(i).second);
        }
        db.addresult(this->id,std::make_unique<AnswerMsgResult>(std::move(max_result)));
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
