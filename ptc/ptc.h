// ==========================================================================
// Author: Benjamin Menkuec <benjamin@menkuec.de>
// License: LGPL
// dont remove this notices
// ==========================================================================
#pragma once

#include <future>
#include <functional>
#include "semaphore.h"

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

    template <typename T1, typename T2>
    struct mypair
    {
        using first_type = T1;
        using second_type = T2;
        T1 first;
        T2 second;

        mypair(const T1& t1, const T2& t2) : first(t1), second(t2) {};
    };

    namespace OrderPolicy
    {
        struct Unordered {};
        struct Ordered {};
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

    template <typename TOrderPolicy>
    struct OrderManager{
        template <typename TItem>
        using ItemIdPair_t = TItem;
    };

    template<>
    struct OrderManager<OrderPolicy::Unordered>
    {
    public:
        OrderManager(unsigned int) {};
        template <typename TItem>
        using ItemIdPair_t = TItem;

        template <typename TItem>
        auto appendOrderId(TItem item) -> TItem
        {
            return item;
        }
        template <typename TItem>
        auto extractItem(TItem&& item) -> TItem
        {
            return std::move(item);
        }
        template <typename TNewItem>
        bool accept_item(TNewItem&&) const noexcept {
            return true;
        }

        template <typename TItem>
        bool is_next_item(TItem item){
            return item != nullptr;
        }

        template <typename TTransformer, typename TItemIdPair>
        static auto callTransformer(TTransformer& transformer, TItemIdPair&& itemIdPair)
        {
            return std::move(transformer(std::move(*itemIdPair)));
        }
    };

    template<>
    struct OrderManager<OrderPolicy::Ordered>
    {
    private:
        using id_t = unsigned int;
        std::atomic<id_t> id;
        unsigned int _numSlots;
    public:
        template <typename TItem>
        using ItemIdPair_t = mypair<TItem*, id_t>;

        OrderManager(const unsigned int numSlots) : id(0), _numSlots(numSlots) {};
        template <typename TItem>
        auto appendOrderId(TItem item) -> mypair<TItem, id_t>*        {
            // can used relaxed here, because this function is always called
            // from the same thread
            auto current_id = id.load(std::memory_order_relaxed);   
            //std::cout << "start id: " << current_id << std::endl;
            id.fetch_add(1, std::memory_order_relaxed);
            return new mypair<TItem, id_t>(item, current_id);
        }

        template <typename TItem>
        auto extractItem(std::unique_ptr<TItem>&& itemIdPair) 
        {
            std::unique_ptr<std::remove_pointer_t<typename TItem::first_type>> temp(itemIdPair->first);
            return std::move(temp);
        }

        template <typename TNewItem>
        bool accept_item(TNewItem&& newItem) const noexcept {
            return (newItem->second - id.load(std::memory_order_acquire)) < _numSlots;
        }

        template <typename TItem>
        bool is_next_item(TItem item)
        {
            if (item != nullptr && item->second == id.load(std::memory_order_relaxed))
            {
                id.fetch_add(1, std::memory_order_release);
                return true;
            }
            return false;
        }

        template <typename TTransformer, typename TItemIdPair>
        static auto callTransformer(TTransformer& transformer, TItemIdPair&& itemIdPair) {
            std::unique_ptr<std::remove_pointer_t<decltype(itemIdPair->first)>> item;
            item.reset(itemIdPair->first);
            auto& newItem = transformer(std::move(*item));
            std::unique_ptr<ItemIdPair_t<typename std::remove_reference_t<decltype(newItem)>::element_type>> newItemIdPair;
            newItemIdPair.reset(reinterpret_cast<ItemIdPair_t<typename std::remove_reference_t<decltype(newItem)>::element_type>*>(itemIdPair.release()));
            newItemIdPair->first = newItem.release();
            return std::move(newItemIdPair);
        }
    };


    // reads read sets from hd and puts them into slots, waits if no free slots are available
    template<typename TSource, typename TOrderPolicy, typename TWaitPolicy>
    struct Produce : private OrderManager<TOrderPolicy>
    {
        using core_item_type = typename std::result_of_t<TSource()>::element_type;
        using item_type = ItemIdPair_t<core_item_type>;
        //using item_type = ItemIdPair_t<typename std::result_of_t<TSource()>::element_type>;

    private:
        TSource& _source;
        unsigned int _numSlots;
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
        void waitForItem(WaitPolicy::Spin) {};

        void waitForSlot(WaitPolicy::Sleep) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));
        }
        void waitForSlot(WaitPolicy::Semaphore) {
            slotEmptySemaphore.wait();
        }


    public:
        Produce(TSource& source, const unsigned int numSlots, const unsigned int sleepMS = defaultSleepMS)
            : OrderManager(numSlots), _source(source), _numSlots(numSlots), _eof(false), _sleepMS(sleepMS)
        {
            for (auto& item : _tlsItems)
                item.store(nullptr);  // fill initialization does not work for atomics
        }
        ~Produce()
        {
            if (_thread.joinable())
                _thread.join();
        }
        void start()
        {
            /*
            - check if empty slot is available, if yes, read data into it
            - set eof=true if no more data available or max_number_of_reads reached
            - return from thread if eof == true
            - go to sleep if no free slot is available
            */
            _tlsItems = std::vector<std::atomic<item_type*>>(_numSlots); // can not use resize, because TItem is not copyable if it contains unique_ptr
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
                                item.store(appendOrderId(currentItem.release()), std::memory_order_relaxed);
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
        template <typename TConsumer>
        bool getItem(std::unique_ptr<item_type>& returnItem, const TConsumer& consumer) noexcept
        {
            item_type* temp = nullptr;
            bool nothingToDo = true;
            while (true)
            {
                nothingToDo = true;
                bool eof = _eof.load(std::memory_order_acquire);
                for (auto& item : _tlsItems)
                {
                    if ((temp = item.load(std::memory_order_relaxed)) != nullptr)
                    {
                        // keep spinning if item is available, but was not accepted because of order policy
                        nothingToDo = false;
                        if (consumer.accept_item(temp) && item.compare_exchange_strong(temp, nullptr, std::memory_order_relaxed))
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
                if (nothingToDo)
                    waitForItem(TWaitPolicy());
            }
            return false;
        };
    };


    template<typename TSink, typename TCoreItemType, typename TOrderPolicy, typename TWaitPolicy>
    struct Consume : public OrderManager<TOrderPolicy>
    {
    public:
        using item_type = ItemIdPair_t<TCoreItemType>;
        using ownSink = std::is_same<TSink, std::remove_reference_t<TSink>>;    // not used
    private:
        TSink& _sink;
        unsigned int _numSlots;
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
        void waitForItem(WaitPolicy::Spin) {};

        void waitForSlot(WaitPolicy::Sleep) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));}
        void waitForSlot(WaitPolicy::Semaphore) {
            slotEmptySemaphore.wait();}


    public:
        Consume(TSink&& sink, const unsigned int numSlots, const unsigned int sleepMS = defaultSleepMS)
            : OrderManager(numSlots), _sink(sink), _numSlots(numSlots), _run(false), _sleepMS(sleepMS)
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

        template<typename = std::result_of_t<decltype(&std::remove_reference_t<TSink>::get_result)(TSink)>>
        std::remove_reference_t<std::result_of_t<decltype(&std::remove_reference_t<TSink>::get_result)(TSink)>>
        get_result()
        {
            return _sink.get_result();
        }

        void start()
        {
            _run = true;
            _tlsItems = std::vector<std::atomic<item_type*>>(_numSlots);
            _thread = std::thread([this]()
            {
                std::unique_ptr<item_type> currentItemIdPair;
                bool nothingToDo = false;
                while (_run.load(std::memory_order_relaxed) || !nothingToDo)
                {
                    nothingToDo = true;
                    for (auto& item : _tlsItems)
                    {
                        if (is_next_item(item.load(std::memory_order_relaxed)))
                        {
                            currentItemIdPair.reset(item.load(std::memory_order_relaxed));
                            item.store(nullptr, std::memory_order_release); // make the slot free again
                            signalSlotAvailable(TWaitPolicy());
                            nothingToDo = false;

                            //std::this_thread::sleep_for(std::chrono::milliseconds(1));  // used for debuggin slow hd case
                            auto temp = extractItem(std::move(currentItemIdPair));
                            _sink(std::move(*temp));
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
        template <typename TItem>
        void pushItem(TItem&& newItem)     // blocks until item could be added
        {
            while (true)
            {
                for (auto& item : _tlsItems)
                {
                    if (item.load(std::memory_order_relaxed) == nullptr)
                    {
                        typename TItem::element_type* temp = nullptr;
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

    template <typename TSource, typename TTransformer, typename TSink, typename TOrderPolicy, typename TWaitPolicy>
    struct PTC_unit
    {
    private:
        // main thread variables
        using Produce_t = Produce<TSource, TOrderPolicy, TWaitPolicy>;
        using produce_core_item_type = typename Produce_t::core_item_type;
        produce_core_item_type test;
        Produce_t _producer;

        TTransformer _transformer;
        using transform_core_item = typename std::result_of_t<std::decay_t<TTransformer>(produce_core_item_type)>::element_type;
        
        using Consume_t = Consume<TSink, transform_core_item, TOrderPolicy, TWaitPolicy>;
        Consume_t _consumer;
        std::vector<std::thread> _threads;

    public:
        PTC_unit(TSource& source, TTransformer& transformer, TSink&& sink, const unsigned int numThreads) :
            _producer(source, numThreads+1), _transformer(transformer), _consumer(std::forward<TSink>(sink), numThreads+1), _threads(numThreads){};

        void start()
        {
            _producer.start();
            _consumer.start();
            for (auto& _thread : _threads)
            {
                _thread = std::thread([this]()
                {
                    std::unique_ptr<typename Produce_t::item_type> item;
                    while (_producer.getItem(item, _consumer))
                    {
                        _consumer.pushItem(std::move(OrderManager<TOrderPolicy>::callTransformer(_transformer, std::move(item))));
                    }
                });
            }
        }

        void wait()
        {
            for (auto& _thread : _threads)
                if (_thread.joinable())
                    _thread.join();
            while (!_consumer.idle())  // wait until all remaining items have been consumed
            {};
            _consumer.shutDown();
        }
        
        template <typename = std::result_of_t<decltype(&std::remove_reference_t<TSink>::get_result)(TSink)>>
        std::future<std::remove_reference_t<std::result_of_t<decltype(&std::remove_reference_t<TSink>::get_result)(TSink)>>>
        get_future()
        {
            auto f = std::async([this]() {
                wait();
                return _consumer.get_result();
            });
            return f;
        }

        bool finished() noexcept
        {
            return _producer.eof();
        }

    };

    template <typename TSource, typename TTransformer, typename TSink>
    auto ordered_ptc(TSource&& source, TTransformer& transformer, TSink&& sink, const unsigned int numThreads)
    {
        return std::make_unique<PTC_unit<TSource, TTransformer, TSink, OrderPolicy::Ordered, WaitPolicy::Semaphore>>
            (std::forward<TSource>(source), transformer, std::forward<TSink>(sink), numThreads);
    }

    template <typename TSource, typename TTransformer, typename TSink>
    auto unordered_ptc(TSource&& source, TTransformer& transformer, TSink&& sink, const unsigned int numThreads)
    {
        return std::make_unique<PTC_unit<TSource, TTransformer, TSink, OrderPolicy::Unordered, WaitPolicy::Semaphore>>
            (std::forward<TSource>(source), transformer, std::forward<TSink>(sink), numThreads);
    }

}
