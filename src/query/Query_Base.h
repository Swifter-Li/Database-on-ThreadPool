//
// Created by dominic on 18-11-17.
//

#ifndef PROJECT_QUERY_BASE_H
#define PROJECT_QUERY_BASE_H


#include "QueryResult.h"

#include <functional>
#include <memory>
#include <string>

class Query {

protected:
    std::string targetTable;


public:
    int id = -1;

    Query() = default;

    explicit Query(std::string targetTable) : targetTable(std::move(targetTable)) {}

    typedef std::unique_ptr<Query> Ptr;

    virtual QueryResult::Ptr execute() = 0;

    virtual std::string toString() = 0;

    virtual QueryResult::Ptr mergeAndPrint() { return nullptr; };

    virtual ~Query() = default;

    void assignid(int counter){
        id = counter;
    }
};

class NopQuery : public Query {
public:
    QueryResult::Ptr execute() override {
        return std::make_unique<NullQueryResult>();
    }

    std::string toString() override {
        return "QUERY = NOOP";
    }
};



#endif //PROJECT_QUERY_BASE_H
