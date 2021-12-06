#pragma once
#include <vector>
#include <map>
#include <mutex>
#include <memory>
#include <future>
#include <iostream>

namespace mqm
{

template<typename Key, typename Value>
struct MqmConsumer
{
    virtual ~MqmConsumer() = default;
    virtual void consume(const Key& id, const Value& value) = 0;
};

template<typename Key, typename Value>
using MqmConsumerPtr = std::shared_ptr<MqmConsumer<Key, Value>>;

// data + signal + stopped flag
template<typename Value>
class MqmSource
{
    std::vector<Value> values_;
    std::mutex mtx_;
    std::condition_variable cv_;
    bool stopped_ = false;

public:
    void enqueue(Value&& v)
    {
        std::unique_lock<std::mutex> lock{ mtx_ };
        if (stopped_)
            throw std::runtime_error("Can't enqueue, queue is stopped");
        values_.emplace_back(std::move(v));
        cv_.notify_one();
    }

    void stop()
    {
        std::unique_lock<std::mutex> lock{ mtx_ };
        stopped_ = true;
        cv_.notify_one();
    }

    bool get(std::vector<Value>& values)
    {
        values.clear();
        std::unique_lock<std::mutex> lock{ mtx_ };
        while (values_.empty() && !stopped_)
            cv_.wait(lock);

        values_.swap(values);
        return stopped_;
    }
};

template<typename Value>
using MqmSourcePtr = std::shared_ptr<MqmSource<Value>>;
template<typename Value>
using MqmSourceWeak = std::weak_ptr<MqmSource<Value>>;

// consumers collection
template<typename Key, typename Value>
class MqmSink
{
    std::vector<MqmConsumerPtr<Key, Value>> consumers_;
    std::mutex consumersMtx_;
    const Key key_;
public:

    MqmSink(const Key& key) : key_(key) { }

    void subscribe(const MqmConsumerPtr<Key, Value>& consumer)
    {
        std::unique_lock<std::mutex> lock{ consumersMtx_ };
        consumers_.push_back(consumer);
    }

    void consume(const std::vector<Value>& values)
    {
        std::unique_lock<std::mutex> lock{ consumersMtx_ };
        for (auto& c : consumers_)
            for (auto& v : values)
                try
                {
                    c->consume(key_, v);
                }
                catch (const std::exception& e)
                {
                    std::cout << "consumer error: " << e.what() << "\n";
                }
    }
};

template<typename Key, typename Value>
using MqmSinkPtr = std::shared_ptr<MqmSink<Key, Value>>;
template<typename Key, typename Value>
using MqmSinkWeak = std::weak_ptr<MqmSink<Key, Value>>;

// consumers collection + std::future
// (I wish it could be specified with some 'task-spawn-strategy',
//  instead of std::async)
template<typename Key, typename Value>
class MqmActiveSink
{
    MqmSinkPtr<Key, Value> sink_;
    std::future<void> task_;
public:
    MqmActiveSink(const Key& key)
        : sink_(std::make_shared<MqmSink<Key, Value>>(key)) { }

    void subscribe(const MqmConsumerPtr<Key, Value>& consumer)
    {
        sink_->subscribe(consumer);
    }

    void start(const MqmSourcePtr<Value>& data)
    {
        MqmSourceWeak<Value> sourceWeak = data;
        MqmSinkWeak<Key, Value> sinkWeak = sink_;
        task_ = std::async(std::launch::async, [sourceWeak, sinkWeak]() {
            std::vector<Value> values;
            for (bool stopped = false; !stopped; )
            try
            {
                auto source = sourceWeak.lock();
                auto sink = sinkWeak.lock();
                if (!source || !sink)
                    return;

                stopped = source->get(values);
                sink->consume(values);
            }
            catch (const std::exception& e)
            {
                std::cout << "task error: " << e.what() << "\n";
            }
        });
    }
};

// sources collection + sinks collection 
template<typename Key, typename Value>
using MqmActiveSinkPtr = std::shared_ptr<MqmActiveSink<Key, Value>>;

template<typename Key, typename Value>
class MqmProcessor
{
    std::map<Key, MqmSourcePtr<Value>> sources_;
    std::mutex sourcesMtx_;

    std::map<Key, MqmActiveSinkPtr<Key, Value>> sinks_;
    std::mutex sinksMtx_;

    MqmSourcePtr<Value> getSource(const Key& key)
    {
        std::unique_lock<std::mutex> lock{ sourcesMtx_ };
        auto ib = sources_.insert({key, nullptr});
        if (ib.second)
            ib.first->second = std::make_shared<MqmSource<Value>>();;
        return ib.first->second;
    }

    void removeSource(const Key& key)
    {
        std::unique_lock<std::mutex> lock{ sourcesMtx_ };
        auto i = sources_.find(key);
        if (i == sources_.end())
            return;
        i->second->stop();
        sources_.erase(i);
    }

    MqmActiveSinkPtr<Key, Value> getSink(const Key& key, bool& created)
    {
        std::unique_lock<std::mutex> lock{ sinksMtx_ };
        auto ib = sinks_.insert({ key, nullptr });
        created = ib.second;
        if (ib.second)
            ib.first->second = std::make_shared<MqmActiveSink<Key, Value>>(key);
        return ib.first->second;
    }

    void removeSink(const Key& key)
    {
        std::unique_lock<std::mutex> lock{ sinksMtx_ };
        sinks_.erase(key);
    }

public:
    ~MqmProcessor()
    {
        for (auto& s : sources_)
            s.second->stop();
    }
    void subscribe(const Key& key, const MqmConsumerPtr<Key, Value>& consumer)
    {
        bool created{};
        auto sink = getSink(key, created);
        sink->subscribe(consumer);
        if (created)
            sink->start(getSource(key));
    }
    void unsubscribe(const Key& key)
    {
        removeSource(key);
        removeSink(key);
    }
    void enqueue(const Key& key, Value&& value)
    {
        getSource(key)->enqueue(std::move(value));
    }
};
}