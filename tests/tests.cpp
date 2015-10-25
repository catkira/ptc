#include <iostream>
#include <string>

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
    const unsigned int numThreads = 10;
    vector<int> vals = {10,20,30,40,50,20,30,40};

    unsigned int count = 0;
    auto myptc = ptc::unordered_ptc(
        [&vals]()->auto {
            if(vals.empty())
                return unique_ptr<int>();
            auto ret = make_unique<int>(vals.back());
            vals.pop_back();
            return ret;
            },
        [](auto&& in)->auto {return make_unique<std::string>(std::to_string(in) + " -> " + std::to_string(fibonacci(in)));},
        [](auto&& in) {cout << in << endl;},
        numThreads
        );
    myptc->start();
    myptc->wait();
    cout << "hello world." << endl;
}



