//
// Created by dominic on 18-11-17.
//

#include "Task.h"

Task::Task(Query *query, Table::Iterator begin, Table::Iterator end, Table *table) : query(query),
                                                                                     begin(begin),
                                                                                     end(end),
                                                                                     table(table), counter(0) {}

Task::Task(Query *query) : query(query), counter(0) {}

Table::SizeType Task::getCounter() {
    return counter;
}

void Task::execute() {
    return;
}