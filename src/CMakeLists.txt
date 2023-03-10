cmake_minimum_required(VERSION 2.7)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-Wall -Wextra -Werror -pedantic -Wno-unused-result -Wno-unused-parameter")

set(SOURCE_FILES
        threadmanage/ThreadPool.cpp threadmanage/ThreadPool.h
        db/Database.cpp db/Database.h
        db/Table.cpp db/Table.h

        utils/formatter.h utils/uexception.h

        query/Query.cpp query/Query.h
        query/QueryParser.cpp query/QueryParser.h
        query/QueryResult.h query/QueryResult.cpp
        query/QueryBuilders.cpp query/QueryBuilders.h

        query/management/LoadTableQuery.cpp query/management/LoadTableQuery.h
        query/management/DropTableQuery.cpp query/management/DropTableQuery.h
        query/management/DumpTableQuery.cpp query/management/DumpTableQuery.h
        query/management/ListTableQuery.cpp query/management/ListTableQuery.h
        query/management/QuitQuery.cpp query/management/QuitQuery.h
        query/management/PrintTableQuery.cpp query/management/PrintTableQuery.h
        query/management/CopyTableQuery.cpp query/management/CopyTableQuery.h

        query/data/InsertQuery.cpp query/data/InsertQuery.h
        query/data/UpdateQuery.cpp query/data/UpdateQuery.h
        query/data/MaxQuery.h query/data/MaxQuery.cpp
        query/data/MinQuery.cpp query/data/MinQuery.h
        query/data/SwapQuery.cpp query/data/SwapQuery.h
        query/management/TruncateTableQuery.cpp query/management/TruncateTableQuery.h
        query/data/AddQuery.cpp query/data/AddQuery.h
        query/data/SubQuery.cpp query/data/SubQuery.h
        query/data/SelectQuery.cpp query/data/SelectQuery.h
        query/data/DeleteQuery.cpp query/data/DeleteQuery.h
        query/data/CountQuery.cpp query/data/CountQuery.h
        query/data/DuplicateQuery.h query/data/DuplicateQuery.cpp
        query/data/SumQuery.cpp query/data/SumQuery.h
        threadmanage/Task.cpp threadmanage/Task.h query/Query_Base.h)

add_executable(lemondb main.cpp ${SOURCE_FILES})
target_link_libraries(lemondb pthread)
