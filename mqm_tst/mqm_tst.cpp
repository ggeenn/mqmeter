#include <iostream>
#include <string>

#include "mqm/mqm.h"

std::atomic<uint64_t> gTotalProcessed;

class TestConsumer : public mqm::MqmConsumer<size_t, std::string>
{
    const std::string name_;
public:
    TestConsumer(const std::string& name) : name_(name) {}
    void consume(const size_t& id, const std::string& value)
    {
        //std::cout << "Consumer[" << name_ << "] ( " << id << ", " << value << ")\n";
        ++gTotalProcessed;
    }
};

int main(int argc, char** argv)
{
    const size_t totalIds = 100;
    const size_t totalMsg = 100500;
    {
        mqm::MqmProcessor<size_t, std::string> processor;
        std::thread producer([&]() {
            for (size_t i = 0; i < totalMsg; ++i)
                processor.enqueue(i % totalIds, "test_msg");
        });

        for (size_t i = 0; i < totalIds; ++i)
            processor.subscribe(i, std::make_shared< TestConsumer >(std::to_string(i)));

        producer.join();
        std::cout << totalMsg << " were sent\n";
    }
    std::cout << gTotalProcessed << " were processed\n";

    return 0;
}
