//
// Created by liu on 18-10-21.
//

#include "query/QueryParser.h"
#include "query/QueryBuilders.h"

#include <time.h>
#include <getopt.h>
#include <fstream>
#include <iostream>
#include <string>

struct {
    std::string listen;
    long threads = 0;
} parsedArgs;

void parseArgs(int argc, char *argv[]) {
    const option longOpts[] = {
            {"listen",  required_argument, nullptr, 'l'},
            {"threads", required_argument, nullptr, 't'},
            {nullptr,   no_argument,       nullptr, 0}
    };
    const char *shortOpts = "l:t:";
    int opt, longIndex;
    while ((opt = getopt_long(argc, argv, shortOpts, longOpts, &longIndex)) != -1) {
        if (opt == 'l') {
            parsedArgs.listen = optarg;
        } else if (opt == 't') {
            parsedArgs.threads = std::strtol(optarg, nullptr, 10);
        } else {
            std::cerr << "lemondb: warning: unknown argument " << longOpts[longIndex].name << std::endl;
        }
    }

}

std::string extractQueryString(std::istream &is) {
    std::string buf;
    do {
        int ch = is.get();
        if (ch == ';') return buf;
        if (ch == EOF) throw std::ios_base::failure("End of input");
        buf.push_back((char) ch);
    } while (true);
}
/*
void queryJob(std::string queryStr, size_t &counter) {
    QueryParser p;

    p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());

    try {
        Query::Ptr query = p.parseQuery(std::move(queryStr));
        QueryResult::Ptr result = query->execute();
        std::cout << ++counter << "\n";
        if (result->success()) {
            if (result->display()) {
                std::cout << *result;
                //std::cout.flush();
            } else {
#ifndef NDEBUG
                std::cout.flush();
                std::cerr << *result;
#endif
                //std::cout.flush();
            }
        } else {
            std::cout.flush();
            std::cerr << "QUERY FAILED:\n\t" << *result;
        }
    } catch (const std::exception &e) {
        std::cout.flush();
        std::cerr << e.what() << std::endl;
    }
}
*/
int main(int argc, char *argv[]) {
    // Assume only C++ style I/O is used in lemondb
    // Do not use printf/fprintf in <cstdio> with this line
    std::ios_base::sync_with_stdio(false);


    parseArgs(argc, argv);

    std::fstream fin;
    if (!parsedArgs.listen.empty()) {
        fin.open(parsedArgs.listen);
        if (!fin.is_open()) {
            std::cerr << "lemondb: error: " << parsedArgs.listen << ": no such file or directory" << std::endl;
            exit(-1);
        }
    }
    std::istream is(fin.rdbuf());

#ifdef NDEBUG
    // In production mode, listen argument must be defined
    if (parsedArgs.listen.empty()) {
        std::cerr << "lemondb: error: --listen argument not found, not allowed in production mode" << std::endl;
        exit(-1);
    }
#else
    // In debug mode, use stdin as input if no listen file is found
    if (parsedArgs.listen.empty()) {
        std::cerr << "lemondb: warning: --listen argument not found, use stdin instead in debug mode" << std::endl;
        is.rdbuf(std::cin.rdbuf());
    }
#endif

    if (parsedArgs.threads < 0) {
        std::cerr << "lemondb: error: threads num can not be negative value " << parsedArgs.threads << std::endl;
        exit(-1);
    } else if (parsedArgs.threads == 0) {
        parsedArgs.threads = std::thread::hardware_concurrency();
        std::cerr << "lemondb: info: auto detect thread num = " << parsedArgs.threads << std::endl;
    } else {
        std::cerr << "lemondb: info: running in " << parsedArgs.threads << " threads" << std::endl;
    }

    //Generate thread pool instance and start
    ThreadPool::initPool(parsedArgs.threads - 1);
    //pool.stop();


    QueryParser p;

    p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());

    size_t counter = 0;

    /*
     * In while loop, call pool.addTask(std::bind(func, arguments))
     *
     * void func(arguments) {
     *      ...
     *      real task such as Query operators
     * }
     */
    Database &db=Database::getInstance();
    std::ifstream listenFile;
    while (is) {
        try {
            // A very standard REPL
            // REPL: Read-Evaluate-Print-Loop
            std::string queryStr = extractQueryString(is);
            if(queryStr.find("LISTEN") != std::string::npos){
                std::istringstream listerParser;
                listerParser.str(queryStr);
                std::string l, b1, path, b2;
                listerParser >> l >> b1 >> path >> b2;
                listenFile.open(path);
                if(!listenFile.is_open()){
                    std::cerr << "Can not open file " << path << std::endl;
                }
                is.rdbuf(listenFile.rdbuf());
                continue;
            }
            Query::Ptr query = p.parseQuery(queryStr);
            auto q = query.get();
            q->assignid(counter+1);
            //std::cerr << counter+1 << "\n";
            //QueryResult::Ptr result = query->execute();
            //db.queries.emplace(counter+1,std::move(query));
            db.queries_push(counter+1, std::move(query));
            //std::cerr << "here\n";
            QueryResult::Ptr result = q->execute();
            //std::cerr << "here2\n";
            //todo@ now output is done in db.exit().
            ++counter;
            if (result->success()) {
                if (result->display()) {
                } else {
#ifndef NDEBUG
                    std::cout.flush();
                    //std::cerr << *result;
#endif
                    //std::cout.flush();
                }
            } else {
                std::cout.flush();
                std::cerr << "QUERY FAILED:\n\t" << *result;
            }
            /*
            std::cout << ++counter << "\n";
            if (result->success()) {
                if (result->display()) {
                    std::cout << *result;
                    //std::cout.flush();
                } else {
#ifndef NDEBUG
                    std::cout.flush();
                    std::cerr << *result;
#endif
                    //std::cout.flush();
                }
            } else {
                std::cout.flush();
                std::cerr << "QUERY FAILED:\n\t" << *result;
            }
             */
        }  catch (const std::ios_base::failure& e) {
            // End of input
            if(listenFile.is_open())
                listenFile.close();
           //Database &db = Database::getInstance();
            db.exit();
            break;
        } catch (const std::exception& e) {
            std::cout.flush();
            std::cerr << e.what() << std::endl;
        }
    }
    if(listenFile.is_open())
        listenFile.close();
    /*
    //todo@ implement threads that run query for each table
    while(is){
        try{
            std::string queryStr = extractQueryString(is);
            Query::Ptr query = p.parseQuery(queryStr);

        }   catch (const std::ios_base::failure& e) {
            // End of input
            break;
        } catch (const std::exception& e) {
            std::cout.flush();
            std::cerr << e.what() << std::endl;
        }
    }
    */
    /*
    while(is){
        try {
            std::string queryStr = extractQueryString(is);
            pool.addTask(std::bind(queryJob, std::move(queryStr), std::ref(counter)));
        }catch (const std::ios_base::failure& e) {
            // End of input
            break;
        }
    }
*/
    return 0;
}