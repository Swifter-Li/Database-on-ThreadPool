//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUERY_H
#define PROJECT_QUERY_H

#include "Query_Base.h"
#include "QueryResult.h"
#include "../db/Table.h"
#include "../threadmanage/Task.h"
#include "../threadmanage/ThreadPool.h"
#include "../db/Database.h"

#include <iostream>
#include <functional>
#include <memory>
#include <string>

struct QueryCondition {
    std::string field;
    size_t fieldId;
    std::string op;
    std::function<bool(const Table::ValueType &, const Table::ValueType &)> comp;
    std::string value;
    Table::ValueType valueParsed;
};

class ComplexQuery : public Query {
protected:
    /** The field names in the first () */
    std::vector<std::string> operands;
    /** The function used in where clause */
    std::vector<QueryCondition> condition;
public:
    typedef std::unique_ptr<ComplexQuery> Ptr;

    /**
     * init a fast condition according to the table
     * note that the condition is only effective if the table fields are not changed
     * @param table
     * @param conditions
     * @return a pair of the key and a flag
     * if flag is false, the condition is always false
     * in this situation, the condition may not be fully initialized to save time
     */
    std::pair<std::string, bool> initCondition(const Table &table);

    /**
     * skip the evaluation of KEY
     * (which should be done after initConditionFast is called)
     * @param conditions
     * @param object
     * @return
     */
    bool evalCondition(const Table::Object &object);

    /**
     * This function seems have small effect and causes somme bugs
     * so it is not used actually
     * @param table
     * @param function
     * @return
     */
    bool testKeyCondition(Table &table, std::function<void(bool, Table::Object::Ptr &&)> function);

    //virtual bool modify() = 0;

    ComplexQuery(std::string targetTable,
                 std::vector<std::string> operands,
                 std::vector<QueryCondition> condition)
            : Query(std::move(targetTable)),
              operands(std::move(operands)),
              condition(std::move(condition)) {
    }

    /** Get operands in the query */
    const std::vector<std::string> &getOperands() const { return operands; }

    /** Get condition in the query, seems no use now */
    const std::vector<QueryCondition> &getCondition() { return condition; }
};

class ConcurrentQuery : public ComplexQuery {
protected:
    std::vector<std::unique_ptr<Task> > subTasks;
    size_t concurrency_num = 1;
    int complete_num = 0;
    std::mutex concurrentLock;
public:
    explicit ConcurrentQuery(std::string targetTable, std::vector<std::string> operands,
                             std::vector<QueryCondition> condition) : ComplexQuery(std::move(targetTable),
                                                                                   std::move(operands),
                                                                                   std::move(condition)) {}

    template<class RealTask>
    void addTaskByPaging(Table &table){
        //todo: Adjust page_size to a proper value
        ThreadPool &threadPool = ThreadPool::getPool();
        unsigned int page_size = 10000;
        size_t total_size = table.size();
        auto begin = table.begin();
        decltype(begin) end;
        {
            std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
            if(total_size == 0){
                end = table.end();
                auto newTask = std::unique_ptr<RealTask>(new RealTask(this, begin, end, &table));
                auto newTaskPtr = newTask.get();
                subTasks.emplace_back(std::move(newTask));
                threadPool.addTask(newTaskPtr);
            } else{
                size_t residue = total_size % page_size;
                if(residue == 0)
                    concurrency_num = total_size / page_size;
                else
                    concurrency_num = total_size / page_size +1;
                for(unsigned long i=0;i<concurrency_num;i++){
                    if(residue != 0 && i == concurrency_num-1){
                        end = table.end();
                    } else
                        end = begin + page_size;
                    auto newTask = std::unique_ptr<RealTask>(new RealTask(this, begin, end, &table));
                    auto newTaskPtr = newTask.get();
                    subTasks.emplace_back(std::move(newTask));
                    //std::cerr << newTaskPtr->query->id<<"sub into task!\n" ;
                    threadPool.addTask(newTaskPtr);
                    begin = end;
                }
            }
        }

    }

    template<class RealTask>
    void addSingleTask(Table &table){
        ThreadPool &threadPool = ThreadPool::getPool();
        std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
        auto newTask = std::unique_ptr<RealTask>(new RealTask(this, table.begin(), table.end(), &table));
        auto newTaskPtr = newTask.get();
        subTasks.emplace_back(std::move(newTask));
        threadPool.addTask(newTaskPtr);
    }
};

#endif //PROJECT_QUERY_H
