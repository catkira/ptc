// ==========================================================================
// Author: Benjamin Menkuec <benjamin@menkuec.de>
// License: LGPL
// dont remove this notices
// ==========================================================================
#pragma once

#include <future>
#include <functional>
#include <list>

#include <boost/lockfree/queue.hpp>

#include "semaphore.h"

namespace ptc
{
    template <typename T1, typename T2>
    struct mypair
    {
        using first_type = T1;
        using second_type = T2;
        T1 first;
        T2 second;

        mypair(T1& t1, T2& t2) : first(std::move(t1)), second(std::move(t2)) {};
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
    };

    template<>
    struct OrderManager<OrderPolicy::Unordered>
    {
    public:
        OrderManager(unsigned int) {};

        template <typename TItem>
        using ItemIdPair_t = typename TItem::element_type;

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

        template <typename TItem>
        inline bool is_next_item(TItem item) noexcept{
            return item != nullptr;
        }

        template <typename TTransformer, typename TItemIdPair>
        static auto callTransformer(const TTransformer& transformer, TItemIdPair&& itemIdPair)
        {
            return std::move(transformer(std::move(itemIdPair)));
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
        using ItemIdPair_t = mypair<TItem, id_t>;

        OrderManager(const unsigned int numSlots) : id(0), _numSlots(numSlots) {};

        unsigned int getId() const noexcept {
            return id.load(std::memory_order_acquire);
        }

        template <typename TItem>
        auto appendOrderId(TItem item){
            // can used relaxed here, because this function is always called
            // from the same thread
            auto current_id = id.load(std::memory_order_relaxed);   
            //std::cout << "start id: " << current_id << std::endl;
            id.fetch_add(1, std::memory_order_relaxed);
            return std::make_unique<mypair<TItem, id_t>>(item, current_id);
        }

        template <typename TItem>
        auto extractItem(std::unique_ptr<TItem>&& itemIdPair) 
        {
            return std::move(itemIdPair->first);
        }

        template <typename TItem>
        inline bool is_next_item(TItem item)
        {
            if (item != nullptr && item->second == id.load(std::memory_order_relaxed))
            {
                id.fetch_add(1, std::memory_order_release);
                return true;
            }
            return false;
        }

        template <typename TTransformer, typename TItemIdPair>
        static auto callTransformer(const TTransformer& transformer, TItemIdPair itemIdPair) {
            auto newItem = transformer(std::move(itemIdPair->first));
            std::unique_ptr<ItemIdPair_t<decltype(newItem)>> newItemIdPair;
            // reuse pair, avoid new call
            newItemIdPair.reset(reinterpret_cast<typename decltype(newItemIdPair)::element_type*>(itemIdPair.release()));
            newItemIdPair->first = std::move(newItem);
            return std::move(newItemIdPair);
        }
    };

    struct WaitManager
    {
    private:
        LightweightSemaphore slotEmptySemaphore;
        LightweightSemaphore itemAvailableSemaphore;

        const unsigned int _sleepMS = 10;
    public:
        void signalSlotAvailable(WaitPolicy::Sleep) {}
        void signalSlotAvailable(WaitPolicy::Semaphore) {
            slotEmptySemaphore.signal();
        }

        void signalItemAvailable(const unsigned int n, WaitPolicy::Sleep) {}
        void signalItemAvailable(const unsigned int n, WaitPolicy::Semaphore) {
                itemAvailableSemaphore.signal(static_cast<int>(n));
        }
        void signalItemAvailable(WaitPolicy::Sleep) {}
        void signalItemAvailable(WaitPolicy::Semaphore) {
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

    };


    enum class InputPolicy
    {
        single,
        multi
    };
    enum class OutputPolicy
    {
        single,
        multi
    };

    template<typename TItem, InputPolicy inputPolicy, OutputPolicy outputPolicy, typename TWaitPolicy>
    struct Slots : private WaitManager
    {
    private:
        std::vector<std::atomic<TItem*>> _items;
    public:
        Slots(const unsigned int numSlots) : _items(numSlots) {};

        bool try_insert(std::unique_ptr<TItem>& insert_item) noexcept {
            for (auto& item : _items)
            {
                if (item.load(std::memory_order_relaxed) == nullptr)
                {
                    if (inputPolicy == InputPolicy::single)
                    {
                        item.store(insert_item.release(), std::memory_order_release);
                        return true;
                    }
                    else
                    {
                        TItem* temp = nullptr;
                        if (item.compare_exchange_strong(temp, insert_item.get()))
                        {
                            insert_item.release();
                            return true;
                        }
                    }
                }
            }
            return false;
        }
        void insert(std::unique_ptr<TItem> item) noexcept {
            while (true)
            {
                if (try_insert(item))
                    return;
                waitForSlot(TWaitPolicy());
            }
        }
        void retrieve(std::unique_ptr<TItem>& item) {
            // not implemented yet
            return true;
        }
        bool try_retrieve(std::unique_ptr<TItem>& retrieve_item) noexcept {
            TItem* temp = nullptr;
            for (auto& item : _items)
            {
                if ((temp = item.load(std::memory_order_relaxed)) != nullptr)
                {
                    if (outputPolicy == OutputPolicy::single)
                    {
                        retrieve_item.reset(temp);
                        item.store(nullptr, std::memory_order_release);
                        signalSlotAvailable(TWaitPolicy());
                        return true;
                    }
                    else
                    {
                        if (item.compare_exchange_strong(temp, nullptr, std::memory_order_release))
                        {
                            retrieve_item.reset(temp);
                            signalSlotAvailable(TWaitPolicy());
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    };

    template<typename TItem, typename TWaitPolicy>
    struct LockfreeQueue : private WaitManager
    {
    private:
        boost::lockfree::queue<TItem*, boost::lockfree::fixed_sized<true>> _queue;
    public:
        LockfreeQueue(const unsigned int numSlots) : _queue(numSlots) {};

        bool try_insert(std::unique_ptr<TItem>& insert_item) noexcept {
            if (_queue.push(insert_item.get()))
                return true;
            return false;
        }
        void insert(std::unique_ptr<TItem> item) noexcept {
            while (true)
            {
                if (_queue.push(item.get()))
                {
                    item.release();
                    return;
                }
                waitForSlot(TWaitPolicy());
            }
        }
        void retrieve(std::unique_ptr<TItem>& item) {
            // not implemented yet
            return true;
        }
        bool try_retrieve(std::unique_ptr<TItem>& retrieve_item) noexcept {
            TItem* temp = nullptr;
            if (_queue.pop(temp))
            {
                retrieve_item.reset(temp);
                signalSlotAvailable(TWaitPolicy());
                return true;
            }
            return false;
        }
    };

    // todo: use CRTP for better interface
    template<typename TItem, InputPolicy inputPolicy, OutputPolicy outputPolicy, typename TWaitPolicy, typename TOrderPolicy>
    struct ContainerSelector : public Slots<TItem, inputPolicy, outputPolicy, TWaitPolicy>
    {
    };

    template<typename TItem, InputPolicy inputPolicy, OutputPolicy outputPolicy, typename TWaitPolicy>
    struct ContainerSelector<TItem, inputPolicy, outputPolicy, TWaitPolicy, OrderPolicy::Unordered> : public Slots<TItem, inputPolicy, outputPolicy, TWaitPolicy>
    {
        ContainerSelector(const unsigned int size) : Slots(size) {};
    };

    // note: using a slot for unordered mode is marginally faster than using a lockfree queue
    //template<typename TItem, InputPolicy inputPolicy, OutputPolicy outputPolicy, typename TWaitPolicy>
    //struct ContainerSelector<TItem, inputPolicy, outputPolicy, TWaitPolicy, OrderPolicy::Unordered> : public LockfreeQueue<TItem, TWaitPolicy>
    //{
    //    ContainerSelector(const unsigned int size) : LockfreeQueue(size) {};
    //};

    template<typename TItem, InputPolicy inputPolicy, OutputPolicy outputPolicy, typename TWaitPolicy>
    struct ContainerSelector<TItem, inputPolicy, outputPolicy, TWaitPolicy, OrderPolicy::Ordered> : public LockfreeQueue<TItem, TWaitPolicy>
    {
        ContainerSelector(const unsigned int size) : LockfreeQueue(size) {};
    };



    // reads read sets from hd and puts them into slots, waits if no free slots are available
    // todo: use lockfree queue for ordered mode instead of slot
    template<typename TSource, typename TOrderPolicy, typename TWaitPolicy>
    struct Produce : private OrderManager<TOrderPolicy>, private WaitManager
    {
        using core_item_type = typename std::result_of_t<TSource()>;
        using item_type = typename OrderManager<TOrderPolicy>::template ItemIdPair_t<core_item_type>;

    private:
        ContainerSelector<item_type, InputPolicy::single, OutputPolicy::multi, TWaitPolicy, TOrderPolicy> _slots;
        TSource& _source;
        unsigned int _numSlots;
        std::thread _thread;
        std::atomic_bool _eof;

    public:
        Produce(TSource& source, const unsigned int numSlots)
            : OrderManager<TOrderPolicy>(numSlots), _slots(numSlots), _source(source), _numSlots(numSlots), _eof(false)
        {}
        ~Produce()
        {
            if (_thread.joinable())
                _thread.join();
        }
        void start()
        {
            /*
            - read data
            - set eof=true if data is null item and return from thread
            - if eof=false, insert data into slot
            */
            _thread = std::thread([this]()
            {
                while (true)
                {
                    auto item = _source();
                    if (!item)
                    {
                        _eof.store(true, std::memory_order_release);
                        signalItemAvailable(_numSlots, TWaitPolicy());
                        return;
                    }
                    auto insert_item = this->appendOrderId(std::move(item));
                    _slots.insert(std::move(insert_item));
                    signalItemAvailable(TWaitPolicy());
                }
            });
        }
        inline bool eof() const noexcept
        {
            return _eof.load(std::memory_order_acquire);
        }
        /*
        do the following steps sequentially
        - check if any slot contains data, if yes return true
        - check if eof is reached and all slots are empty, return false
        - go to sleep until data is available
        */
        bool getItem(std::unique_ptr<item_type>& returnItem) noexcept
        {
            while (true)
            {
                bool eof = _eof.load(std::memory_order_acquire);
                if (!_slots.try_retrieve(returnItem))
                    if (!eof)
                        waitForItem(TWaitPolicy());
                    else
                        return false;
                else
                    return true;
                //std::cout << std::this_thread::get_id() << "-2" << std::endl;
            }
            return false;
        };
    };


    template<typename TSink, typename TCoreItemType, typename TOrderPolicy, typename TWaitPolicy>
    struct Consume : public OrderManager<TOrderPolicy>, private WaitManager
    {
    public:
        using item_type = typename OrderManager<TOrderPolicy>::template ItemIdPair_t<TCoreItemType>;
        using ownSink = std::is_same<TSink, std::remove_reference_t<TSink>>;    // not used
    private:
        ContainerSelector<item_type, InputPolicy::multi, OutputPolicy::single, TWaitPolicy, TOrderPolicy> _slots;
        TSink& _sink;
        unsigned int _numSlots;
        std::thread _thread;
        std::atomic_bool _run;

    public:
        Consume(TSink&& sink, const unsigned int numSlots)
            : OrderManager<TOrderPolicy>(numSlots), _slots(numSlots), _sink(sink), _numSlots(numSlots), _run(false)
        {}
        ~Consume()
        {
            _run = false;
            if (_thread.joinable())
                _thread.join();
        }

        template<typename Sink = TSink, typename = decltype(&std::remove_reference_t<Sink>::get_result)(Sink)>
        auto
        get_result()
        {
            return _sink.get_result();
        }

        void start()
        {
            _run = true;
            _thread = std::thread([this]()
            {
                std::list<std::unique_ptr<item_type>> itemBuffer;
                std::unique_ptr<item_type> currentItemIdPair;
                bool nothingToDo = false;
                while (_run.load(std::memory_order_relaxed) || !itemBuffer.empty())
                {
                    if(std::is_same<TOrderPolicy, OrderPolicy::Ordered>::value && !itemBuffer.empty())  // only in ordered mode
                    {
                        for (auto it = itemBuffer.begin();it != itemBuffer.end();++it)
                        {
                            if (this->is_next_item((*it).get()))
                            {
                                _sink(std::move(this->extractItem(std::move(*it))));
                                it = itemBuffer.erase(it);
                            }
                        }
                    }
                    if (_slots.try_retrieve(currentItemIdPair))
                    {
                        if (this->is_next_item(currentItemIdPair.get()))
                        {
                            auto temp = this->extractItem(std::move(currentItemIdPair));
                            _sink(std::move(temp));
                        }
                        else
                        {
                            itemBuffer.emplace_back(std::move(currentItemIdPair));
                        }
                        signalSlotAvailable(TWaitPolicy());
                    }
                    else if(std::is_same<TOrderPolicy, OrderPolicy::Unordered>::value || itemBuffer.empty())
                        waitForItem(TWaitPolicy());
                }
            });
        }
        template <typename TItem>
        void pushItem(TItem&& newItem)     // blocks until item could be added
        {
            _slots.insert(std::move(newItem));
            signalItemAvailable(TWaitPolicy());
        }
        void shutDown()
        {
            _run.store(false, std::memory_order_relaxed);
            signalItemAvailable(TWaitPolicy());
            if (_thread.joinable())
                _thread.join();
        }
    };

    template <typename TSource, typename TTransformer, typename TSink, typename TOrderPolicy, typename TWaitPolicy>
    struct PTC_unit
    {
    private:
        // producer needs consumer feedback in ordered mode -> slow
        using Produce_t = Produce<TSource, TOrderPolicy, TWaitPolicy>;
        using produce_core_item_type = typename Produce<TSource, TOrderPolicy, TWaitPolicy>::core_item_type;
        Produce_t _producer;

        const TTransformer _transformer;
        using transform_core_item = typename std::result_of_t<TTransformer(produce_core_item_type)>;

        using Consume_t = Consume<TSink, transform_core_item, TOrderPolicy, TWaitPolicy>;
        Consume_t _consumer;

        std::vector<std::thread> _threads;
    public:
        PTC_unit(TSource& source, const TTransformer& transformer, TSink&& sink, const unsigned int numThreads) :
            _transformer(transformer), _consumer(std::forward<TSink>(sink), numThreads+1), _producer(source, numThreads + 1), _threads(numThreads){};

        void start()
        {
            _producer.start();
            _consumer.start();
            for (auto& _thread : _threads)
            {
                _thread = std::thread([this]()
                {
                    std::unique_ptr<typename Produce_t::item_type> item;
                    while (_producer.getItem(item))
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
            _consumer.shutDown();
        }
        
        template <typename Sink = TSink, typename = decltype(&std::remove_reference_t<Sink>::get_result)(Sink)>
        auto
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
    auto ordered_ptc(TSource&& source, const TTransformer& transformer, TSink&& sink, const unsigned int numThreads)
    {
        return std::make_unique<PTC_unit<TSource, TTransformer, TSink, OrderPolicy::Ordered, WaitPolicy::Semaphore>>
            (std::forward<TSource>(source), transformer, std::forward<TSink>(sink), numThreads);
    }

    template <typename TSource, typename TTransformer, typename TSink>
    auto unordered_ptc(TSource&& source, const TTransformer& transformer, TSink&& sink, const unsigned int numThreads)
    {
        return std::make_unique<PTC_unit<TSource, TTransformer, TSink, OrderPolicy::Unordered, WaitPolicy::Semaphore>>
            (std::forward<TSource>(source), transformer, std::forward<TSink>(sink), numThreads);
    }

}
