#include <iostream>
#include <string>

#include "mqm/mqm.h"


class TestConsumer : public mqm::MqmConsumer<size_t, std::string>
{
    std::atomic <size_t>& total_;
public:
    TestConsumer(std::atomic <size_t>& total) : total_(total) {}
    void consume(const size_t& id, const std::string& value)
    {
        ++total_;
    }
};

int main(int argc, char** argv)
{
    const size_t totalIds = 100;
    const size_t totalMsg = 100500;
    std::atomic <size_t> totalProcessed;
    {
        mqm::MqmProcessor<size_t, std::string> processor;
        std::thread producer([&]() {
            for (size_t i = 0; i < totalMsg; ++i)
                processor.enqueue(i % totalIds, "test_msg");
        });

        for (size_t i = 0; i < totalIds; ++i)
            processor.subscribe(i, std::make_shared< TestConsumer >(totalProcessed));

        producer.join();
        std::cout << totalMsg << " were sent\n";
    }
    std::cout << totalProcessed << " were processed\n";

    return 0;
}
