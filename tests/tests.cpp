#include <iostream>
#include <string>
#include <vector>
#include <chrono>

#include <boost/algorithm/string.hpp>

#include "../ptc/ptc.h"

using namespace std;

unsigned int fibonacci(unsigned int n) {
    if (n == 0)
        return 0;
    else if (n == 1)
        return 1;
    else
        return fibonacci(n - 1) + fibonacci(n - 2);
}

int main()
{
    unsigned int numThreads = 10;
    std::cout << "\nnum threads: " << numThreads << endl;

    vector<int> vals = { 10,20,30,40,42,20,30,40 };

    cout << "ordered" << endl;
    auto producer = [&vals]()->auto {
            if(vals.empty())
                return unique_ptr<int>();
            auto ret = make_unique<int>(vals.back());
            vals.pop_back();
            return ret;
            };
    auto consumer = [](auto in) {cout << *in << endl;};

    auto myoptc = ptc::ordered_ptc(
        // produce
        [&vals]()->auto {
        if (vals.empty())
            return unique_ptr<int>();
        auto ret = make_unique<int>(vals.back());
        vals.pop_back();
        return ret;
        },
        // transform
        [](auto in)->auto {return make_unique<std::string>("fib(" + std::to_string(*in) + ") = " + std::to_string(fibonacci(*in)));},
        // consume
        [](auto in) {cout << *in << endl;},
        numThreads
        );
    myoptc->start();
    myoptc->wait();

    vals = { 10,20,30,40,42,20,30,40 };
    cout << "unordered" << endl;
    auto myptc = ptc::unordered_ptc(
        // produce
        producer,
        // transform
        [](auto in)->auto {return make_unique<std::string>("fib(" + std::to_string(*in) + ") = " + std::to_string(fibonacci(*in)));},
        // consume
        consumer,
        numThreads
        );
    myptc->start();
    myptc->wait();

    // example for memory reuse

    unsigned count = 1000;
    auto producerReuse = [&count](std::unique_ptr<std::string>&& usedItem)->auto {
        if (count == 0)
            return unique_ptr<std::string>();
        --count;
        if (usedItem == nullptr)  // necessary for first element
            usedItem = make_unique<std::string>();
        usedItem->assign("Hello World");
        return std::move(usedItem);
    };
    auto myptcReuse = ptc::unordered_ptc(
        // produce
        producerReuse,
        // transform
        [](auto in)->auto {boost::to_upper(*in); return in;},
        // consume
        [](auto in) {cout << *in << endl; return in;},
        numThreads
        );
    myptcReuse->start();
    myptcReuse->wait();

    // benchmarks

    const unsigned max_count = 1000000;
    count = max_count;
    auto produce_n_items = [&count]()->auto {
        if (count == 0)
            return unique_ptr<int>();
        auto ret = make_unique<int>(0);
        --count;
        return ret;
    };
    auto consumer_do_nothing = [](auto in) {volatile typename decltype(in)::element_type a = *in; (void)a;};
    auto ptc_unordered_benchmark = ptc::unordered_ptc(
        // produce
        produce_n_items,
        // transform
        [](auto in)->auto {return std::move(in);},
        // consume
        consumer_do_nothing,
        numThreads
        );

    cout << endl;
    cout << "inserting 1 million ints in unordered mode (my slots)...";
    auto t_start = std::chrono::steady_clock::now();
    ptc_unordered_benchmark->start();
    ptc_unordered_benchmark->wait();
    auto t_end = std::chrono::steady_clock::now();
    auto deltaTime = std::chrono::duration_cast<std::chrono::duration<float>>(t_end - t_start).count();
    cout << endl << "done in " << deltaTime << "s"<<endl;

    auto ptc_unordered_queue_benchmark = ptc::unordered_use_queue_ptc(
        // produce
        produce_n_items,
        // transform
        [](auto in)->auto {return std::move(in);},
        // consume
        consumer_do_nothing,
        numThreads
        );
    cout << "inserting 1 million ints in unordered mode (boost lockfree queue)...";
    t_start = std::chrono::steady_clock::now();
    count = max_count;
    ptc_unordered_queue_benchmark->start();
    ptc_unordered_queue_benchmark->wait();
    t_end = std::chrono::steady_clock::now();
    deltaTime = std::chrono::duration_cast<std::chrono::duration<float>>(t_end - t_start).count();
    cout << endl << "done in " << deltaTime << "s" << endl;

    auto ptc_ordered_benchmark = ptc::ordered_ptc(
        // produce
        produce_n_items,
        // transform
        [](auto in)->auto {return std::move(in);},
        // consume
        consumer_do_nothing,
        numThreads
        );
    cout << "inserting 1 million ints in ordered mode (boost lockfree queue)...";
    t_start = std::chrono::steady_clock::now();
    count = max_count;
    ptc_ordered_benchmark->start();
    ptc_ordered_benchmark->wait();
    t_end = std::chrono::steady_clock::now();
    deltaTime = std::chrono::duration_cast<std::chrono::duration<float>>(t_end - t_start).count();
    cout << endl << "done in " << deltaTime << "s" << endl;

    numThreads = 1000;
    std::cout << "\nnum threads: " << numThreads << endl;

    auto ptc_unordered_benchmark2 = ptc::unordered_ptc(
        // produce
        produce_n_items,
        // transform
        [](auto in)->auto {return std::move(in);},
        // consume
        consumer_do_nothing,
        numThreads
        );

    cout << endl;
    cout << "inserting 1 million ints in unordered mode (my slots)...";
    t_start = std::chrono::steady_clock::now();
    count = max_count;
    ptc_unordered_benchmark2->start();
    ptc_unordered_benchmark2->wait();
    t_end = std::chrono::steady_clock::now();
    deltaTime = std::chrono::duration_cast<std::chrono::duration<float>>(t_end - t_start).count();
    cout << endl << "done in " << deltaTime << "s" << endl;

    auto ptc_unordered_queue_benchmark2 = ptc::unordered_use_queue_ptc(
        // produce
        produce_n_items,
        // transform
        [](auto in)->auto {return std::move(in);},
        // consume
        consumer_do_nothing,
        numThreads
        );
    cout << "inserting 1 million ints in unordered mode (boost lockfree queue)...";
    t_start = std::chrono::steady_clock::now();
    count = max_count;
    ptc_unordered_queue_benchmark2->start();
    ptc_unordered_queue_benchmark2->wait();
    t_end = std::chrono::steady_clock::now();
    deltaTime = std::chrono::duration_cast<std::chrono::duration<float>>(t_end - t_start).count();
    cout << endl << "done in " << deltaTime << "s" << endl;

    auto ptc_ordered_benchmark2 = ptc::ordered_ptc(
        // produce
        produce_n_items,
        // transform
        [](auto in)->auto {return std::move(in);},
        // consume
        consumer_do_nothing,
        numThreads
        );
    cout << "inserting 1 million ints in ordered mode (boost lockfree queue)...";
    t_start = std::chrono::steady_clock::now();
    count = max_count;
    ptc_ordered_benchmark2->start();
    ptc_ordered_benchmark2->wait();
    t_end = std::chrono::steady_clock::now();
    deltaTime = std::chrono::duration_cast<std::chrono::duration<float>>(t_end - t_start).count();
    cout << endl << "done in " << deltaTime << "s" << endl;



    cout << "\nHello world, I am done." << endl;
}



