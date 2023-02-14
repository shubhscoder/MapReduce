/*
This is a thread-safe implementation of a queue.
The locking used in this implementation is coarse grained, and the lock is acquired
on the entire container. This is not appropriate for performance, but in this application,
we are concerned with correctness rather than performance.

Also, the majority overhead in map-reduce is the computation that takes place on the
workers and the communication cost associated with those communication. This queue
is used in master's control path, which should not serve as a bottleneck.
*/

#include <thread>
#include <mutex>
#include <queue>
#include <memory>
#include <chrono>
#include <condition_variable>

using namespace std::chrono_literals;

using std::queue;
using std::mutex;
using std::shared_ptr;
using std::condition_variable;
using std::unique_lock;
using std::move;
using std::lock_guard;
using std::chrono;
using std::make_shared;

template<typename T>
class ConcurrentQueue {
    private:

    // A mutex to protect the underlying STL queue.
    mutable mutex mut;

    // Underlying STL data structure of the concurrent queue.
    queue<shared_ptr<T> > data_queue;

    // This condition variable is necessary, because, we want to wait for
    // some work to be added to the queue. Also, we cannot wait infinitely
    // and wait for only some time before checking again.
    condition_variable data_cond;

    // Time for which the condition variable should wait, before returning nullptr.
    chrono::duration wait_time = std::chrono::milliseconds(100);

    public:
    ConcurrentQueue() {}

    // This function will wait for wait_time seconds. If no task is found in the queue
    // it will a nullptr. Handling nullptr is the responsibility of the application.
    shared_ptr<T> wait_and_pop() {
        unique_lock<mutex> lk(mut);
        if (!data_cond.wait_for(lk, wait_time, [this] {return !data_queue.empty();})) {
            return nullptr;
        }
        shared_ptr<T> result = data_queue.front();
        data_queue.pop();
        return result;
    }

    // Add a value to the queue, and notify the condition variable of the addition.
    void Push(T new_value) {
        shared_ptr<T> data(make_shared<T> (move(new_value)));
        lock_guard<mutex> lk(mut);
        data_queue.push(data);
        data_cond.notify_one();
    }

    // Check if the queue is empty.
    bool empty() {
        lock_guard<mutex> lk(mut);
        return data_queue.empty();
    }

    // Returns the number of elements in the queue.
    int32_t size() {
        lock_guard<mutex> lk(mut);
        return data_queue.size();
    }
};