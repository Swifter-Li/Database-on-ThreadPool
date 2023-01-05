//
// Created by camelboat on 18-11-1.
//

#ifndef PROJECT_COPYTABLEQUERY_H
#define PROJECT_COPYTABLEQUERY_H

#include "../Query.h"

class CopyTableQuery : public Query {
    static constexpr const char *qname = "COPY";
    const std::string new_table;
public:

    explicit  CopyTableQuery(std::string origin_table, std::string new_table)
        : Query(std::move(origin_table)), new_table(std::move(new_table)) {}

            QueryResult::Ptr execute() override;

            std::string toString() override;
};

#endif //PROJECT_COPYTABLEQUERY_H
