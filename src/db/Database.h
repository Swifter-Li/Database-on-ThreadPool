//
// Created by liu on 18-10-23.
//

#ifndef PROJECT_DB_H
#define PROJECT_DB_H

#include "../threadmanage/ThreadPool.h"
#include "Table.h"

#include <memory>
#include <unordered_map>
#include <mutex>

// open timer for all queries
// #define TIMER

class Database {
private:
    /**
     * A unique pointer to the global database object
     */
    static std::unique_ptr<Database> instance;

    /**
     * The map of tableName -> table unique ptr
     */
    std::unordered_map<std::string, Table::Ptr> tables;

    std::recursive_mutex tables_lock;

    /**
     * The map of fileName -> tableName
     */
    std::unordered_map<std::string, std::string> fileTableNameMap;

    std::mutex fileTableNameMap_lock;

    /**
     * The map to store multithread results
     */
    std::unordered_map<int, QueryResult::Ptr > queryresults;

    /**
     * The default constructor is made private for singleton instance
     */
    Database() = default;

    /**
     * The lock for add table lock
     */
    std::mutex table_lock_mutex;

    /**
     * The lock for queries
     */
    std::mutex queries_mutex;

    /**
     * The lock for queryresults
     */
    std::mutex queryresults_mutex;

public:
    std::unordered_map<int,Query::Ptr> queries;

    std::unordered_map<std::string,std::unique_ptr<std::mutex>> table_locks;

    void add_table_lock(std::string tablename){
        //std::mutex lock;
        std::unique_lock<std::mutex> lock(this->table_lock_mutex);
        this->table_locks.emplace(tablename,std::make_unique<std::mutex>());
    }

    void addresult(int id,QueryResult::Ptr &&queryresult){
        std::unique_lock<std::mutex> lock(this->queryresults_mutex);
        this->queryresults.emplace(id,std::move(queryresult));
    }

    Query::Ptr queries_erase(const int id) {
        std::unique_lock<std::mutex> locker(queries_mutex);
        auto &db = Database::getInstance();
        Query::Ptr tmp = std::move(db.queries[id]);
        db.queries.erase(id);
        return tmp;
    };

    void queries_push(const int id, Query::Ptr q) {
        std::unique_lock<std::mutex> locker(queries_mutex);
        auto &db = Database::getInstance();
        db.queries.emplace(id, std::move(q));
    }

    void testDuplicate(const std::string &tableName);

    Table &registerTable(Table::Ptr &&table);

    void dropTable(const std::string &tableName);

    //for truncate
    void truncateTable(const std::string &tableName);

    void printAllTable();

    Table &operator[](const std::string &tableName);

    const Table &operator[](const std::string &tableName) const;

    Database &operator=(const Database &) = delete;

    Database &operator=(Database &&) = delete;

    Database(const Database &) = delete;

    Database(Database &&) = delete;

    ~Database() = default;

    static Database &getInstance();

    void updateFileTableName(const std::string &fileName, const std::string &tableName);

    std::string getFileTableName(const std::string &fileName);

    std::string loadTableNameFromStream(std::istream &is, std::string source = "");

    /**
     * Load a table from an input stream (i.e., a file)
     * @param is
     * @param source
     * @return reference of loaded table
     */
    Table &loadTableContentFromStream(std::istream &is, std::string source = "");


    void exit();
};


#endif //PROJECT_DB_H
