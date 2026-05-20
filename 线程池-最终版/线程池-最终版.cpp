// 线程池-最终版.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <iostream>
#include"threadpool.h"
#include<chrono>

int sum1(int x, int y)
{
    this_thread::sleep_for(chrono::seconds(2));
    return x + y;
}

int main()
{
    ThreadPool pool;
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start(2);
    future<int>r2 =pool.submitTask(sum1, 1, 2);
    future<int>r1 = pool.submitTask(sum1, 1, 2);
    future<int>r3 = pool.submitTask(sum1, 1, 2);
    future<int>r4 = pool.submitTask(sum1, 1, 2);
    future<int>r5 = pool.submitTask(sum1, 1, 2);
    cout << r1.get() << endl;
    cout << r2.get() << endl;

}
