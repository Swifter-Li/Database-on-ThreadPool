//
// Created by dominic on 18-11-11.
//

#ifndef PROJECT_THREADPOOL_H
#define PROJECT_THREADPOOL_H

#include "Task.h"

#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <vector>
#include <deque>


class ThreadPool {
public:
    //typedef std::function<void(void)> Task;

    static std::unique_ptr<ThreadPool> threadpool;

    explicit ThreadPool(long num);

    ThreadPool(const ThreadPool &) = delete;

    ThreadPool(ThreadPool &&) = delete;

    ThreadPool &operator=(const ThreadPool &) = delete;

    ThreadPool &operator=(ThreadPool &&) = delete;

    void start();

    void stop();

    void addTask(Task *task);

    ~ThreadPool();

    static ThreadPool &initPool (long num);

    /*
     * Should call initPool first and then can get the global thread pool anywhere
     */
    static ThreadPool &getPool ();

private:
    long thread_num;
    bool isrunning;
    std::mutex lock;
    std::condition_variable cond;
    //todo: Use pointer in taskset;
    std::deque<Task *> taskset;
    std::vector<std::shared_ptr<std::thread> > pool;


    void worker();
};


#endif //PROJECT_THREADPOOL_H
