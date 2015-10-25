// ==========================================================================
// Author: Benjamin Menkuec <benjamin@menkuec.de>
// License: LGPL
// dont remove this notices
// ==========================================================================
#pragma once

#include <future>
#include <functional>
#include "semaphore.h"

//#include "helper_functions.h"

namespace ptc
{
    template<typename F, typename Ret, typename A, typename... Rest>
    A helper(Ret(F::*)(A, Rest...));

    template<typename F, typename Ret, typename A, typename... Rest>
    A helper(Ret(F::*)(A, Rest...) const);
    
    template<typename F>
    struct first_argument {
        using type = decltype(helper(&F::operator()));
    };


    namespace WaitPolicy
    {
        struct Sleep {};
        struct Semaphore {};
        struct Spin {};
    };
    constexpr unsigned int defaultSleepMS = 10;

    /*
    http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n4058.pdf
    wait until this proposal gets accepted, then use atomic<unique_ptr<...>>
    */

    // reads read sets from hd and puts them into slots, waits if no free slots are available
    template<typename TSource, typename TWaitPolicy>
    struct Produce
    {
        using item_type = typename std::result_of<TSource()>::type::element_type;

    private:
        TSource& _source;
        std::vector<std::atomic<item_type*>> _tlsItems;
        std::thread _thread;
        std::atomic_bool _eof;
        unsigned int _sleepMS;
        LightweightSemaphore slotEmptySemaphore;
        LightweightSemaphore itemAvailableSemaphore;

        void signalSlotAvailable(WaitPolicy::Sleep) {}
        void signalSlotAvailable(WaitPolicy::Semaphore) {
            slotEmptySemaphore.signal();
        }

        void signalItemAvailable(const bool eof, WaitPolicy::Sleep) {}
        void signalItemAvailable(const bool eof, WaitPolicy::Semaphore){
            if (eof)  // wakeup all potentially waiting threads so that they can be joined
                itemAvailableSemaphore.signal(_tlsItems.size());
            else
                itemAvailableSemaphore.signal();
        }

        void waitForItem(WaitPolicy::Sleep) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));
        }
        void waitForItem(WaitPolicy::Semaphore) {
            itemAvailableSemaphore.wait();
        }

        void waitForSlot(WaitPolicy::Sleep) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));
        }
        void waitForSlot(WaitPolicy::Semaphore) {
            slotEmptySemaphore.wait();
        }


    public:
        Produce(TSource& source, unsigned int sleepMS = defaultSleepMS)
            : _source(source), _eof(false), _sleepMS(sleepMS)
        {
            for (auto& item : _tlsItems)
                item.store(nullptr);  // fill initialization does not work for atomics
        }
        ~Produce()
        {
            if (_thread.joinable())
                _thread.join();
        }
        void start(const unsigned int numSlots)
        {
            /*
            - check if empty slot is available, if yes, read data into it
            - set eof=true if no more data available or max_number_of_reads reached
            - return from thread if eof == true
            - go to sleep if no free slot is available
            */
            _tlsItems = std::vector<std::atomic<item_type*>>(numSlots); // can not use resize, because TItem is not copyable if it contains unique_ptr
            _thread = std::thread([this]()
            {
                bool noEmptySlot = true;
                while (true)
                {
                    noEmptySlot = true;
                    for (auto& item : _tlsItems)
                    {
                        if (item.load(std::memory_order_relaxed) == nullptr)
                        {
                            noEmptySlot = false;
                            auto currentItem = _source();
                            if (currentItem)
                                item.store(currentItem.release(), std::memory_order_relaxed);
                            else
                                _eof.store(true, std::memory_order_release);

                            signalItemAvailable(_eof.load(std::memory_order_relaxed), TWaitPolicy());
                        }
                        if (_eof.load(std::memory_order_relaxed))
                            return;
                    }
                    if (noEmptySlot)  // no empty slow was found, wait a bit
                    {
                        waitForSlot(TWaitPolicy());
                    }
                }
            });
        }
        inline bool eof() const noexcept
        {
            return _eof.load(std::memory_order_acquire);
        }
        bool idle() noexcept
        {
            if (!_eof.load(std::memory_order_acquire))
                return false;
            for (auto& item : _tlsItems)
                if (item.load(std::memory_order_relaxed))
                    return false;
            return true;
        }
        /*
        do the following steps sequentially
        - check if any slot contains data, if yes take data out and return true
        - check if eof is reached, if yes return false
        - go to sleep until data is available
        */
        bool getItem(std::unique_ptr<item_type>& returnItem) noexcept
        {
            item_type* temp = nullptr;
            while (true)
            {
                bool eof = _eof.load(std::memory_order_acquire);
                for (auto& item : _tlsItems)
                {
                    if ((temp = item.load(std::memory_order_relaxed)) != nullptr)
                    {
                        if (item.compare_exchange_strong(temp, nullptr, std::memory_order_relaxed))
                        {
                            returnItem.reset(temp);
                            signalSlotAvailable(TWaitPolicy());
                            return true;
                        }
                    }
                }
                //std::cout << std::this_thread::get_id() << "-2" << std::endl;
                if (eof) // only return if _eof == true AND all the slots are empty -> therefore read eof BEFORE checking the slots
                    return false;
                waitForItem(TWaitPolicy());
            }
            return false;
        };
    };


    template<typename TSink, typename TWaitPolicy>
    struct Consume
    {
    public:
        using item_type = typename first_argument<TSink>::type::element_type;
    private:
        TSink& _sink;
        std::vector<std::atomic<item_type*>> _tlsItems;
        std::thread _thread;
        std::atomic_bool _run;
        unsigned int _sleepMS;
        LightweightSemaphore itemAvailableSemaphore;
        LightweightSemaphore slotEmptySemaphore;

        void signalItemAvailable(WaitPolicy::Sleep) {}
        void signalItemAvailable(WaitPolicy::Semaphore){
            itemAvailableSemaphore.signal();}

        void signalSlotAvailable(WaitPolicy::Sleep) {}
        void signalSlotAvailable(WaitPolicy::Semaphore) {
            slotEmptySemaphore.signal();}

        void waitForItem(WaitPolicy::Sleep) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));}
        void waitForItem(WaitPolicy::Semaphore) {
            itemAvailableSemaphore.wait();}

        void waitForSlot(WaitPolicy::Sleep) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));}
        void waitForSlot(WaitPolicy::Semaphore) {
            slotEmptySemaphore.wait();}


    public:
        Consume(TSink& sink, unsigned int sleepMS = defaultSleepMS)
            : _sink(sink), _run(false), _sleepMS(sleepMS)
        {
            for (auto& item : _tlsItems)
                item.store(nullptr);  // fill initialization does not work for atomics
        }
        ~Consume()
        {
            _run = false;
            if (_thread.joinable())
                _thread.join();
        }
        void start(const unsigned int numSlots)
        {
            _run = true;
            _tlsItems = std::vector<std::atomic<item_type*>>(numSlots);
            _thread = std::thread([this]()
            {
                std::unique_ptr<item_type> currentItem;
                bool nothingToDo = false;
                while (_run.load(std::memory_order_relaxed) || !nothingToDo)
                {
                    nothingToDo = true;
                    for (auto& item : _tlsItems)
                    {
                        if (item.load(std::memory_order_relaxed) != nullptr)
                        {
                            currentItem.reset(item.load(std::memory_order_relaxed));
                            item.store(nullptr, std::memory_order_release); // make the slot free again
                            signalSlotAvailable(TWaitPolicy());
                            nothingToDo = false;

                            //std::this_thread::sleep_for(std::chrono::milliseconds(1));  // used for debuggin slow hd case
                            _sink(std::move(currentItem));
                        }
                    }
                    if (nothingToDo)
                    {
                        //std::cout << std::this_thread::get_id() << "-4" << std::endl;
                        waitForItem(TWaitPolicy());
                    }
                }
            });
        }
        void pushItem(std::unique_ptr<item_type> newItem)     // blocks until item could be added
        {
            while (true)
            {
                for (auto& item : _tlsItems)
                {
                    if (item.load(std::memory_order_relaxed) == nullptr)
                    {
                        item_type* temp = nullptr;
                        if (item.compare_exchange_strong(temp, newItem.get(), std::memory_order_acq_rel))  // acq_rel to make sure, that idle does not return true before all reads are written
                        {
                            newItem.release();
                            signalItemAvailable(TWaitPolicy());
                            return;
                        }
                    }
                }
                //std::cout << std::this_thread::get_id() << "-3" << std::endl;
                waitForSlot(TWaitPolicy());
            }
        }
        void shutDown()
        {
            _run.store(false, std::memory_order_relaxed);
            signalItemAvailable(TWaitPolicy());
            if (_thread.joinable())
                _thread.join();
        }
        bool idle() noexcept
        {
            for (auto& item : _tlsItems)
                if (item.load(std::memory_order_acquire) != nullptr) // acq to make sure, that idle does not return true before all reads are written
                    return false;
            return true;
        }
    };

    template <typename TSource, typename TTransformer, typename TSink, typename TWaitPolicy>
    struct PTC_unit
    {
    private:
        // main thread variables
        Produce<TSource, TWaitPolicy> _producer;
        TTransformer _transformer;
        Consume<TSink, TWaitPolicy> _consumer;
        std::vector<std::thread> _threads;

    public:
        PTC_unit(TSource& source, TTransformer& transformer, TSink& sink, const unsigned int numThreads) :
            _producer(source), _transformer(transformer), _consumer(sink), _threads(numThreads) {};

        void start()
        {
            _producer.start(_threads.size() + 1);
            _consumer.start(_threads.size() + 1);
            for (auto& _thread : _threads)
            {
                _thread = std::thread([this]()
                {
                    std::unique_ptr<typename Produce<TSource, TWaitPolicy>::item_type> item;
                    while (_producer.getItem(item))
                    {
                        _consumer.pushItem(_transformer(std::move(item)));
                    }
                });
            }
        }
        void waitForFinish()
        {
            for (auto& _thread : _threads)
                if (_thread.joinable())
                    _thread.join();
            while (!_consumer.idle())  // wait until all remaining items have been consumed
            {
            };
            _consumer.shutDown();
        }
        bool finished() noexcept
        {
            return _producer.eof();
        }

    };


    template <typename TSource, typename TTransformer, typename TSink>
    auto unordered_ptc(TSource& source, TTransformer& transformer, TSink& sink, const unsigned int numThreads)
    {
        return std::make_unique<PTC_unit<TSource, TTransformer, TSink, WaitPolicy::Semaphore>>(source, transformer, sink, numThreads);
    }

}
