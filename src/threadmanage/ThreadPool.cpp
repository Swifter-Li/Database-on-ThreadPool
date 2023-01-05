//
// Created by dominic on 18-11-11.
//

#include "ThreadPool.h"
#include <stdio.h>

std::unique_ptr<ThreadPool> ThreadPool::threadpool = nullptr;

ThreadPool::ThreadPool(long num): thread_num(num), isrunning(false) {}

ThreadPool::~ThreadPool() {
    if(isrunning)
        stop();
}

void ThreadPool::start() {
    isrunning = true;
    pool.reserve((unsigned long)thread_num);
    for(int i = 0; i < thread_num; i++){
        pool.emplace_back(std::make_shared<std::thread>(&ThreadPool::worker, this));
    }
}

void ThreadPool::stop() {
    {
        std::unique_lock<std::mutex> locker(lock);
        isrunning = false;
        cond.notify_all();
    }

    for(long i = 0; i < thread_num; ++i){
        auto thread_t = pool[i];
        if(thread_t->joinable())
            thread_t->join();
    }
}

void ThreadPool::addTask(Task *task) {
    if(isrunning){
        std::unique_lock<std::mutex> locker(lock);
        taskset.emplace_back(task);
        cond.notify_one();
    }
}

void ThreadPool::worker() {
    while(isrunning){
        std::unique_lock<std::mutex> locker(lock);
        if (isrunning && taskset.empty())
                cond.wait(locker);
        if (!taskset.empty()) {
            auto task = taskset.front();
            taskset.pop_front();
            locker.unlock();
            if(task) {
                try {
                    task->execute();
                } catch (std::exception &e) {
                        fprintf(stderr, "%s %s", e.what(), task->query->toString().c_str());
                }
            }
        }
    }
}

ThreadPool &ThreadPool::initPool(long num) {
    if(ThreadPool::threadpool == nullptr) {
        threadpool = std::unique_ptr<ThreadPool>(new ThreadPool(num));
        threadpool->start();
    }
    return *threadpool;
}

ThreadPool &ThreadPool::getPool() {
    return *threadpool;
}
