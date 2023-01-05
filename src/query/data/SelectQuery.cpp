//
// Created by dominic on 18-10-29.
//

#include "SelectQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <map>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *SelectQuery::qname;

QueryResult::Ptr SelectQuery::execute() {
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
                "No operand (? operands)."_f % operands.size()
        );
    }
    try {
        if(db.table_locks.find(this->targetTable)==db.table_locks.end()){
            throw TableNameNotFound(
                    "Error accesing table \"" + this->targetTable + "\". Table not found."
            );
        }
        db.table_locks[this->targetTable]->lock();
        auto &table = db[this->targetTable];
        if (this->operands[0] != "KEY") {

            db.addresult(this->id,std::make_unique<ErrorMsgResult>(qname, "Invalid Argument."));
            db.table_locks[this->targetTable]->unlock();
            Query::Ptr tmp = db.queries_erase(this->id);
            return make_unique<ErrorMsgResult>(
                    qname, "The beginning field is not KEY"
            );
        }
        auto result = initCondition(table);
        //map<Table::KeyType, vector<Table::ValueType *> > selectAnswer;
        //cerr<<result.second<<endl;
        if (result.second) {
            /*
            for (auto table_it = table.begin(); table_it != table.end(); ++table_it) {
                if (this->evalCondition(*table_it)) {
                    vector<Table::ValueType *> datum;
                    datum.reserve(table.field().size());
                    for (auto field_it = ++this->operands.begin(); field_it != this->operands.end(); ++field_it) {
                        datum.emplace_back(&((*table_it)[*field_it]));
                    }
                    selectAnswer.emplace(make_pair(table_it->key(), move(datum)));
                }
            }
             */
            addTaskByPaging<SelectTask>(table);
        }
        else{
            db.table_locks[this->targetTable]->unlock();
            db.addresult(this->id, make_unique<SuccessMsgResult>(qname, targetTable));
            Query::Ptr tmp = db.queries_erase(this->id);
        }
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"SELECT takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return std::make_unique<SuccessMsgResult>(qname);
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

std::string SelectQuery::toString() {
    return "QUERY = SELECT " + this->targetTable + "\"";
}

QueryResult::Ptr SelectQuery::mergeAndPrint() {
    Database &db = Database::getInstance();
    std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
    ++complete_num;
    if(complete_num < (int)concurrency_num){
        return std::make_unique<NullQueryResult>();
    }
    for(const auto &task:subTasks){
        auto real_task = dynamic_cast<SelectTask *>(task.get());
        for(auto answer_it = real_task->localAnswer.begin(); answer_it != real_task->localAnswer.end(); ++answer_it){
            selectAnswer.emplace(std::move(*answer_it));
        }
    }
    if(!selectAnswer.empty())
        db.addresult(this->id,std::make_unique<AnswerMsgResult>(std::move(selectAnswer)));
    else
        db.addresult(this->id,std::make_unique<SuccessMsgResult>(qname));
    db.table_locks[this->targetTable]->unlock();
    Query::Ptr tmp = db.queries_erase(this->id);
    return std::make_unique<NullQueryResult>();
}

void SelectTask::execute() {
    using namespace std;
    auto real_query = dynamic_cast<SelectQuery *>(query);
    for (auto table_it = begin; table_it != end; ++table_it) {
        if (real_query->evalCondition(*table_it)) {
            vector<Table::ValueType *> datum;
            datum.reserve(table->field().size());
            for (auto field_it = ++real_query->operands.begin(); field_it != real_query->operands.end(); ++field_it) {
                datum.emplace_back(&((*table_it)[*field_it]));
            }
            localAnswer.emplace(make_pair(table_it->key(), move(datum)));
        }
    }
    real_query->mergeAndPrint();
}
