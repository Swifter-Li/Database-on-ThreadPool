//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "AddQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>
#ifdef TIMER
#include <iostream>

#endif

constexpr const char *AddQuery::qname;

QueryResult::Ptr AddQuery::execute() {

    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    if (this->operands.empty()) {
        db.addresult(this->id, std::make_unique<ErrorMsgResult>(qname, "Operands Error."));
        Query::Ptr tmp = db.queries_erase(this->id);
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    }

    try {
        if (db.table_locks.find(this->targetTable) == db.table_locks.end()) {
            throw TableNameNotFound(
                    "Error accesing table \"" + this->targetTable + "\". Table not found."
            );
        }
        db.table_locks[this->targetTable]->lock();
        this->add_src.reserve(this->operands.size() - 1);
        auto &table = db[this->targetTable];

        for (auto it = this->operands.begin(); it != this->operands.end(); ++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for ADD.)"_f
                );
            } else {
                if (it + 1 != this->operands.end()) {
                    add_src.emplace_back(table.getFieldIndex(*it));
                } else add_des = table.getFieldIndex(*it);
            }
        }
        auto result = initCondition(table);


        if (result.second) {
            addTaskByPaging<AddTask>(table);
        } else {
            db.addresult(this->id, std::make_unique<RecordCountResult>(0));
            db.table_locks[this->targetTable]->unlock();
            Query::Ptr tmp = db.queries_erase(this->id);
        }
        return make_unique<SuccessMsgResult>(qname);


        /*
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    Table::ValueType sum = 0;
                    for (auto srcitr = add_src.begin();srcitr!=add_src.end();++srcitr) {
                        sum += (*it)[*srcitr];
                    }
                    (*it)[add_des] = sum;
                    ++counter;
                }
            }
            db.table_locks[this->targetTable]->unlock();
        }
        */

#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"ADD takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
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

void AddTask::execute() {
    auto real_query = dynamic_cast<AddQuery *>(query);

    for (auto it = begin; it != end; ++it) {
        if (real_query->evalCondition(*it)) {
            Table::ValueType sum = 0;
            for (auto srcitr = real_query->add_src.begin();srcitr!=real_query->add_src.end();++srcitr) {
                sum += (*it)[*srcitr];
            }
            {
                std::unique_lock<std::mutex> lock(real_query->write_mutex);
                (*it)[real_query->add_des] = sum;
            }
            ++this->counter;
        }
    }
/*
    {
        std::unique_lock<std::mutex> lock(real_query->g_mutex);
        real_query->counter += this->counter;
    }
    */
    real_query->mergeAndPrint();
}

QueryResult::Ptr AddQuery::mergeAndPrint() {
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
    db.addresult(this->id,std::make_unique<RecordCountResult>(counter));

    db.table_locks[this->targetTable]->unlock();
    Query::Ptr tmp = db.queries_erase(this->id);
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

std::string AddQuery::toString() {
    return "QUERY = ADD " + this->targetTable + "\"";
}
