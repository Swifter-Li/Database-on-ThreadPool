//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "SubQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

#include <iostream>
#include <algorithm>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *SubQuery::qname;

QueryResult::Ptr SubQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    if (this->operands.empty()) {

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
        //std::cerr<<this->targetTable <<"    "<< this->id<< "beforelock!\n"  ;
        db.table_locks[this->targetTable]->lock();
        //std::cerr<<this->targetTable <<"    "<< this->id<< "afterlock!\n"  ;
        //std::cerr << "sub after lock\n";
        this->sub_src.reserve(this->operands.size()-2);
        auto &table = db[this->targetTable];
        //std::cerr<<this->targetTable <<"    "<< this->id<< "addtask1!\n"  ;
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for SUB.)"_f
                );
            }
            else {
                if (it == this->operands.begin()) {
                    sub_victim = table.getFieldIndex(*it);
                }
                else if (it+1 != this->operands.end()) {
                    sub_src.emplace_back(table.getFieldIndex(*it));
                }
                else sub_des = table.getFieldIndex(*it);
            }
        }
        //std::cerr<<this->targetTable <<"    "<< this->id<< "addtask2!\n"  ;
        auto result = initCondition(table);
        //std::cerr<<this->targetTable <<"    "<< this->id<< "addtask3!\n"  ;

        if (result.second) {
            //std::cerr<<this->targetTable <<"    "<< this->id<< "addtask4!\n"  ;
            addTaskByPaging<SubTask>(table);
        }
        else{
            db.addresult(this->id,std::make_unique<RecordCountResult>(0));
            db.table_locks[this->targetTable]->unlock();
            Query::Ptr tmp = db.queries_erase(this->id);
        }
        return make_unique<SuccessMsgResult>(qname);


        /*
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    Table::ValueType res = (*it)[sub_victim];
                    for (auto srcitr = sub_src.begin();srcitr!=sub_src.end();++srcitr) {
                        res -= (*it)[*srcitr];
                    }
                    (*it)[sub_des] = res;
                    ++counter;
                }
            }
        }
        */

#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"SUB takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        //db.table_locks[this->targetTable]->unlock();
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



void SubTask::execute() {
    auto real_query = dynamic_cast<SubQuery *>(query);
    for (auto it = begin; it != end; ++it) {
        if (real_query->evalCondition(*it)) {
            Table::ValueType res = (*it)[real_query->sub_victim];
            for (auto srcitr = real_query->sub_src.begin();srcitr!=real_query->sub_src.end();++srcitr) {
                res -= (*it)[*srcitr];
            }
            {
                std::unique_lock<std::mutex> lock(real_query->write_mutex);
                (*it)[real_query->sub_des] = res;
            }

            ++this->counter;
        }
    }
/*
    {
        std::unique_lock<std::mutex> lock(real_query->g_mutex);
        real_query->counter += this->local_counter;
    }
    */
    real_query->mergeAndPrint();
}

QueryResult::Ptr SubQuery::mergeAndPrint() {
    //std::cerr<<this->targetTable <<"    "<< this->id<< "oneofthem!\n"  ;
    Database &db = Database::getInstance();
    //auto &table = db[this->targetTable];
    std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
    ++complete_num;
    if(complete_num < (int)concurrency_num){
        return std::make_unique<NullQueryResult>();
    }
    for(const auto &task:subTasks){
        counter += task->getCounter();
    }
    //std::cerr<<this->targetTable <<"    "<< this->id<< "endstilllock!\n"  ;
    db.addresult(this->id,std::make_unique<RecordCountResult>(counter));
    db.table_locks[this->targetTable]->unlock();
    Query::Ptr tmp = db.queries_erase(this->id);
    //std::cerr<<this->targetTable <<"    "<< this->id<< "unlock!\n"  ;
    //allow the next query to go
    //std::cout<<"table lock released\n";
#ifdef TIMER
    clock_gettime(CLOCK_MONOTONIC, &ts2);
    std::cerr<<"MAX takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
    //db.queries[this->id].reset();
    return std::make_unique<NullQueryResult>();
}

std::string SubQuery::toString() {
    return "QUERY = SUB " + this->targetTable + "\"";
}
