# LemonDB
A multi-threaded database for VE482 project 2.

## Introduction

This is a multi-thread version of LemonDB based on its recovered version. We have implemented functions for data manipulation and also implemented utility of reading queries from a new file. Please find the detailed description for these functions in [p2.pdf](./p2.pdf).

Besides these functions, we have also enabled multi-thread manipulation. The design and performance improvements of our database will be introduced below. We have also included common problems related to multi-threading and other suggestions to help you with the work of LemonDB in the future.

## Sample Usage

`./lemondb --listen test.query --threads 8` 

## Codes & Files

- `./src`
   This contains the source code for LemonDB. `./src/db` contains the implementation for database and table. `./src/query` contains the implementation for database's manipulation. `./src/threadmanage` contains the implementation for threadpool and task division. `./src/utils` contains source code for formatting and exception detection.
- `./bin`
   This contains the origin binary for comparison.
- `./db`
   This contains sample database files.
- `./sample`
   This contains sample queries.

## Database Design
- The database is designed to support at most 8 threads. Threadspool is used here to decrease the unnecessary cost in creating and destroying threads.
- For each query, it is parsed by parseArgs and then stored in queries. After all the quries are read, the corresponding task will be executed by the corresponding objects. Tasks in queries would be executed in parallel.
- For task which doesn't modify the database(such as calculation function `SUM` or search function `MAX`), after detecting the syntax error and getting the instance of table, it will call `addTaskByPaging()` to automatically be divided  into multi-threads by pages. This single task will then run in parallel by calling `execute()` on these pages. After the task is finished on every pages, the final one will call `mergeAndPrint()` to gather the result for each pages into the final result, and it will then call `make_unique<>` to output the result. The default page size in the current database is 5000 data per page.
- Mutex is used to avoid multiple task from reading or writing to the database when other task is writing to the database. `std::unique_lock<std::mutex>` is used as the mutex. When a task wants to read or write the data, it will first call this function to ask for the lock. When the data is unlocked, it will lock the data, read or write on the data and unlock it when it finishes the task.
- Other syntax error would be correctly detected and thrown by catch.
- When `LISTEN` is called, the system will use a thread to manipulate this task, open, read and close the file by the file path. After the reading is done, the file would be closed to avoid influencing other tasks' performance.

## Performance Improvements
- When implementing the DELETE function, we improve the performance by using cache to prevent changing the address of data for each element's deletion. To implement this, we use `loadToCache()` to put the data that would not be deleted into cache, and use `erase_marked()` to mark the data that should be deleted. After the data is iterated, `updateByCache()` is called to use `std::swap()` for swapping original data and cache in order to finish the delete task.
- ​Frequent creation and destruction of threads for short-lived tasks may cause latency in execution, so that implementing threadpool in this project can save a lot of time. Threads in threadpool will not be terminated until the system is terminated by `QUIT`.

## Common Problems & Suggestions
- When calling `COPYTABLE`, if the data is not locked when executing this task, the following query, especially `QUIT` may cause segmentation fault. Also, for `COPYTABLE`, lock should be implemented for both original table and new table.


- Before implementing any ideas or functions, the possible problems among multi-threading should be considered as much and detailed as possible since it could be really difficult to modify the multi-threading architecture after implementing several functions.
- Deadlock problem may occur after executing an input queries files of  comparably large size. Although it is rare in normal testing, it could be really difficult when detecting this problem and it must happen under the same sequence of queries, so that considering the corresponding problems would be necessary. More specific, programmer should make sure that each lock would be returned when the task is finished.
- Situation that no data in the database can conform the evaluation must be considered. If the data is not unlocked when `evalCondition()` is false for all data, the database may be locked forever.



## Copyright

Lemonion Inc. 2018
