#pragma once

#ifndef  THREADPOOL_H
#define  THREADPOOL_H
#include<vector>
#include<queue>
#include<memory>
#include<atomic>
#include<mutex>
#include<condition_variable>
#include<functional>
#include<unordered_map>
#include<thread>
#include<future>
#include<chrono>
#include<iostream>;
using namespace std;

const int TaskMax = 2;      //任务上线
const int threadMax = 300;     //线程上线
const int THREAD_MAX_IDLE_TIME = 10;

//sfdfs
enum class PoolMode {
	MODE_FIXED,    //固定数量
	MODE_CACHED		//线程数量可增长
};



class Thread {
public:
	//线程函数对象类型
	using ThreadFunc = function<void(int)>;
	
	//线程构造
	 Thread(ThreadFunc func_) :func(func_)
		, threadId(generateId++)
	{}


	//线程析构
	~Thread() = default;


	//启动线程
	void start()
	{
		thread t(func, threadId);    //线程对象t和线程函数func
		t.detach();  //设置分离线程
	}


	//获取线程ID
	int getId() const
	{
		return threadId;
	}
private:
	ThreadFunc func;
	static int generateId;
	int threadId;   //保存线程ID
};

int Thread::generateId = 0;

class ThreadPool
{
public:
	//线程池构造
	// 线程池构造
	ThreadPool()
		: initThreadSize(4),
		taskSize(0),
		taskQueMaxTreashHold(TaskMax),
		poolMode(PoolMode::MODE_FIXED),
		isStart(false),
		idleThreadsize(0),
		threadSizeMax(threadMax)
	{}


	//线程池析构
	~ThreadPool()
	{
		isStart = false;

		//等待线程池里面所有的线程返回   两种状态：阻塞 & 执行任务中
		unique_lock<mutex> lock(taskQueMtx);
		notEmpty.notify_all();
		exitCond.wait(lock, [&]()-> bool {return threads.size() == 0; });

	}

	//开启线程池
	void start(int size)
	{
		isStart = true;
		initThreadSize = size;
		totalThread = size;
		//创建线程对象
		for (int i = 0; i < initThreadSize; ++i) {
			auto ptr = make_unique<Thread>(bind(&ThreadPool::threadFunc, this, placeholders::_1));
			int threadId = ptr->getId();
			threads.emplace(threadId, move(ptr));
		}
		for (int i = 0; i < initThreadSize; ++i) {
			threads[i]->start();
			idleThreadsize++;  //空闲线程数量加一
		}
	}


	//设置线程池的工作模式
	void setMode(PoolMode mode)
	{
		if (checkPoolCond()) {
			return;
		}
		poolMode = mode;
	}


	//设置初始线程池的数量
	void setInitThreadSize(int size);

	//给线程池提交任务
	//使用可变参模板编程
	template<typename Func, typename... Args>
	auto submitTask(Func&& func, Args&&... args) -> future<decltype(func(args...))>
	{
		using RType = decltype(func(args...));
		auto task = make_shared<packaged_task<RType()>>(
			bind(forward<Func>(func), forward<Args>(args)...));
		future<RType> result = task->get_future();
		
		unique_lock<mutex> lock(taskQueMtx);
		if (!notFull.wait_for(lock, chrono::seconds(1), [&]() ->bool {return taskQue.size() < taskQueMaxTreashHold; })) {
			cout << "submit filed,taskQue if full..." << endl;
			auto task = make_shared<packaged_task<RType()>>(
				[]()->RType {return RType(); }
			);
			(*task)();
			return task->get_future();
		}

		taskQue.emplace([task]() {(*task)(); });
		taskSize++;
		notEmpty.notify_all();


		if (poolMode == PoolMode::MODE_CACHED)
		{

			if (taskSize > idleThreadsize && totalThread < threadSizeMax) {
				cout << "create new thread..." << endl;
				//创建新线程
				//auto ptr = make_unique<Thread>(bind(&ThreadPool::threadFunc, this, placeholders::_1));
				auto ptr = make_unique<Thread>(
					Thread::ThreadFunc(std::bind
					(&ThreadPool::threadFunc, this, std::placeholders::_1)));
				int threadId = ptr->getId();
				threads.emplace(threadId, move(ptr));
				threads[threadId]->start();  //启动线程
				//修改线程个数相关的变量
				totalThread++;
				idleThreadsize++;
			}
		}



		return result;
	}

	//设置任务队列上线阈值
	void setTaskQueMaxThreshHole(int threshhold)
	{
		taskQueMaxTreashHold = threshhold;
	}

	//设置cached模式下线程数量上线阈值
	void setThreadSizeThreshHold(int threshhold)
	{
		if (!checkPoolCond()) {
			return;
		}
		if (poolMode == PoolMode::MODE_CACHED)
			idleThreadsize = threshhold;
	}



	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;
private:
	//定义线程函数
	void threadFunc(int threadid)
	{
		auto lastTime = chrono::high_resolution_clock().now();
		for (;;)
		{

			Task task;
			// 获取锁
			{
				unique_lock<mutex> lock(taskQueMtx);
				//cached模式下，超过60秒回收线程
				while (taskQue.size() == 0) {
					if (!isStart)
					{
						threads.erase(threadid);
						cout << "threadid：" << this_thread::get_id() << "exit" << endl;
						totalThread--;
						exitCond.notify_all();
						return;
					}
					if (poolMode == PoolMode::MODE_CACHED) {
						//条件变量超时返回
						if (cv_status::timeout ==
							notEmpty.wait_for(lock, chrono::seconds(1)))
						{
							auto now = chrono::high_resolution_clock().now();
							auto dur = chrono::duration_cast<chrono::seconds>(now - lastTime);
							if (dur.count() >= THREAD_MAX_IDLE_TIME && totalThread > initThreadSize) {
								//开始回收线程
								threads.erase(threadid);
								totalThread--;
								idleThreadsize--;

								cout << "threadid：" << this_thread::get_id() << "exit" << endl;
								return;
							}
						}
					}
					else {
						notEmpty.wait(lock);
					}
				}
				if (!isStart) {
					break;
				}
				idleThreadsize--;
				task = taskQue.front();
				taskQue.pop();
				taskSize--;
				if (taskQue.size() > 0) {
					notEmpty.notify_all();
				}
				cout << "尝试获取任务..."<<endl;
				notFull.notify_all();
			}
			if (task != nullptr)
			{
				cout << "获取任务成功..." << endl;
				task();
			}
			lastTime = chrono::high_resolution_clock().now();
			idleThreadsize++;
		}

	}

	bool checkPoolCond()
	{
		return isStart;
	}


private:
	//vector<unique_ptr<Thread>> threads;   //线程列表
	unordered_map<int, unique_ptr<Thread>> threads;   //线程列表
	size_t initThreadSize;   //初始的线程数量
	int threadSizeMax;       //线程数量上线阈值
	atomic_int totalThread;         //记录线程总数量
	atomic_int idleThreadsize;   //记录空闲线程数量

	using Task = function<void()>;
	queue<Task> taskQue;  //	任务队列
	atomic_uint taskSize;   //任务的数量
	int taskQueMaxTreashHold; //任务队列的上线

	mutex taskQueMtx;  //保证任务队列的线程安全
	condition_variable notFull;    //表示任务队列不满
	condition_variable notEmpty;   //表示任务队列不空
	condition_variable exitCond;   //等待


	PoolMode poolMode; //当前线程池的工作状态
	atomic_bool isStart;   //表示当前线程池的启动状态

};


#endif // ! THREADPOOL_H
