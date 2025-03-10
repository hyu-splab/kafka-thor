#include <mosquitto.h>  
#include </home/hss544/json.hpp>  
#include <librdkafka/rdkafkacpp.h>  
#include <condition_variable>  
#include <iostream>
#include <map>  
#include <string>
#include <mutex>  
#include <vector>  
#include <queue>
#include <sstream>  
#include <thread>
#include <unistd.h>
#include <cstdio>  
#include <cstring>  
#include <sys/types.h>  
#include <sys/socket.h>  
#include <netinet/in.h>  
#include <arpa/inet.h>
#include <chrono>  
#include <iomanip>
#include "/home/hss544/rapidjson/include/rapidjson/document.h"
#include "/home/hss544/rapidjson/include/rapidjson/writer.h"
#include "/home/hss544/rapidjson/include/rapidjson/stringbuffer.h"
#include <fstream>
#include <stdlib.h>
#include <atomic>
#include <unordered_map>
#include </home/hss544/librdkafka/src/rdkafka.h>
#include <algorithm>
////////////////////////////////////////////////////////////////// Headers //////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////// Namespaces /////////////////////////////////////////////////////////////////
using namespace std;
using namespace std::chrono;  
using namespace rapidjson;
using json = nlohmann::json;  

class MQTTClient;
class Service;
class Message;
class ServiceManager;
template <typename T> class BlockingQueue;

const int SENSOR_INFO_INTERVAL = 100;
const int INFORMATION_SHARING_INTERVAL = 100;
const int PLATOONING_LOWEST_INTERVAL = 25;
const int PLATOONING_LOWER_INTERVAL = 20;
const int PLATOONING_HIGH_INTERVAL = 10;
const int COLLISION_AVOIDANCE_INTERVAL = 10;

struct ServiceStats {
    int success = 0;
    int fail = 0;
    int success_avg = 0;
    int fail_avg = 0;
};

atomic<int> callback_cnt{0};  // Count number of poll() calls per second
int car_num = 0;
int c_cnt = 0;
int m_cnt = 0;
int max_cnt = 0;
int sec_cnt = 0;
int p_cnt = 0;
int w_cnt =0;
double batch_ms[4] = {0};
double TimeC[4] = {0};
double TimeP[4] = {0};

rd_kafka_s *rks[4];
RdKafka::Topic* topics[4];

string service_names[4] = {
    "Sensor_Sharing",
    "Information_Sharing",
    "Platooning_Lowest",
    "Platooning_Lower"
};

int service_hz[4] = {
    10,
    10,
    33,
    50
};

int service_deadline[4] = {
    100,
    100,
    25,
    20
};

int service_payload[4] = {
    1600,
    6500,
    400,
    6500
};

double service_reliability[4] = {
    0.99,
    0.99,
    0.9,
    0.9
};

int service_cnts[4] = {
    0
};

//////////////////////////////////////////////////////////////////
// Message Class
//////////////////////////////////////////////////////////////////

class Message {
public:
    char* topic;        // 메시지 토픽(CarID)
    char* payload;      // 메시지 페이로드(Data)
    steady_clock::time_point PollTime;     // Time when first poll happened
    steady_clock::time_point ProduceTime;  // Time when first poll happened
    steady_clock::time_point ReturnTime;   // Time when first poll happened
};

//////////////////////////////////////////////////////////////////
// Queue Class
//////////////////////////////////////////////////////////////////

class Queue {
public:
    std::deque<Message*> messages;
    std::mutex queue_mutex;
    std::condition_variable cv;
    bool is_locked = false;
    std::atomic<size_t> message_count{0};
};

//////////////////////////////////////////////////////////////////
// BlockingQueueCollection
//////////////////////////////////////////////////////////////////

class BlockingQueueCollection {
private:
    std::unordered_map<std::string, Queue> queues;  // Map: queueName -> Queue
    std::vector<std::string> queueNames;            // List of queue names for round-robin
    std::mutex container_mutex;                     // Only used for queueNames & map modifications
    std::atomic<size_t> currentIndex{0};            // Round-robin index (atomic to avoid data races)
    size_t capacity;
    size_t sz;

    std::mutex global_mutex;
    std::condition_variable global_cv;

public:
    BlockingQueueCollection(size_t cap) : capacity(cap) {}

    // Create a new queue
    void New_Queue(const std::string& str) {
        std::lock_guard<std::mutex> lock(container_mutex);
        if (queues.find(str) == queues.end()) {
            queues[str];  // Create a new Queue object
            queueNames.push_back(str);
            sz++;
        }
    }

    void put(const std::string& queueName, Message* value) {
        {
            std::lock_guard<std::mutex> lock(container_mutex);
            if (queues.find(queueName) == queues.end()) {
                std::cerr << "Error: Queue '" << queueName << "' does not exist.\n";
                return;
            }
        }

        Queue& queue = queues[queueName];
        {
            std::unique_lock<std::mutex> qlock(queue.queue_mutex);

            queue.cv.wait(qlock, [&] {
                return queue.messages.size() < capacity;
            });

            queue.messages.push_back(value);

            size_t oldCount = queue.message_count.fetch_add(1, std::memory_order_relaxed);
            if (oldCount == 0) {
                std::lock_guard<std::mutex> lk(global_mutex);
                global_cv.notify_one();
            }
        }

        queue.cv.notify_one();
    }

    // Take an item from a queue (round-robin selection)
    Message* take() {
        while (true) {
            {
                std::unique_lock<std::mutex> lk(global_mutex);
                global_cv.wait(lk, [&] {
                    for (auto & name : queueNames) {
                        if (queues[name].message_count.load(std::memory_order_relaxed) > 0) {
                            return true;
                        }
                    }
                    return false;
                });
            }

            size_t startIdx = currentIndex.fetch_add(1, std::memory_order_relaxed) % sz;
            bool found = false;

            for (size_t i = 0; i < sz; i++) {
                size_t idx = (startIdx + i) % sz;

                std::string qName;
                {
                    std::lock_guard<std::mutex> lock(container_mutex);
                    qName = queueNames[idx];
                }

                Queue& queue = queues[qName];

                if (queue.is_locked || queue.message_count.load(std::memory_order_acquire) == 0) {
                    continue;
                }

                if (!queue.queue_mutex.try_lock()) {
                    continue;
                }

                if (!queue.messages.empty() && !queue.is_locked) {
                    Message* msg = queue.messages.front();
                    queue.messages.pop_front();
                    queue.message_count.fetch_sub(1, std::memory_order_relaxed);
                    queue.is_locked = true;
                    found = true;
                    queue.queue_mutex.unlock();
                    return msg;
                } else {
                    queue.queue_mutex.unlock();
                }
            }

            if (!found) {
                std::this_thread::yield();
            }
        }
        return nullptr;
    }

    void reset_queue(const std::string& queueName) {
        if (queues.find(queueName) == queues.end()) {
            std::cerr << "Error: Queue '" << queueName << "' does not exist.\n";
            return;
        }
        Queue& queue = queues[queueName];
        std::unique_lock<std::mutex> lock(queue.queue_mutex);
        queue.is_locked = false;  // Unlock queue for next processing
        queue.cv.notify_one();    // Notify waiting workers
    }
};

BlockingQueueCollection BQ(10000);

//////////////////////////////////////////////////////////////////
// MessageWithTimestamp Struct
//////////////////////////////////////////////////////////////////

struct MessageWithTimestamp {
    string* message;        // Pointer to Kafka message
    int64_t arrival_time;   // Exact time when the message arrived

    MessageWithTimestamp(string* msg, int64_t time)
        : message(msg), arrival_time(time) {}
};

//////////////////////////////////////////////////////////////////
// DeliveryReportCallback Class
//////////////////////////////////////////////////////////////////

class DeliveryReportCallback : public RdKafka::DeliveryReportCb {
public:
    int c_cnt = 1;     
    int car_num;       
    int hz;            
    double total = 0;  
    int id;            

    DeliveryReportCallback(int car_num, int hz, int id)
        : c_cnt(1), car_num(car_num), hz(hz), id(id) {}

    void dr_cb(RdKafka::Message &message) override {
        if (message.err()) {
            std::cerr << "Delivery failed: " << message.errstr() << "\n";
            return;
        }
        if(c_cnt%(car_num)==0){
        int64_t commitTimeMs = message.timestamp().timestamp;
        const RdKafka::Headers* headers = message.headers();
        int64_t produceTimeMs = 0;
        auto headerList = headers->get("produce_time");
        if (!headerList.empty()) {
            produceTimeMs = std::stoll(std::string(
                static_cast<const char*>(headerList[0].value()),
                headerList[0].value_size()
            ));
        }
        int64_t gapMs = (commitTimeMs > 0 && produceTimeMs > 0)
                            ? (commitTimeMs - produceTimeMs)
                            : -1;
        int currentCount = c_cnt++;
        double adjustedGap = ceil(static_cast<double>(gapMs+1) - batch_ms[id]);
        if(adjustedGap<=0) adjustedGap=1;
        if(adjustedGap>service_deadline[id]) adjustedGap=1;
        TimeP[id] = std::max(ceil(TimeP[id]), adjustedGap);
        c_cnt==0;
        }
        c_cnt++;
    }
};

std::vector<DeliveryReportCallback*> cb_list;
bool running = true;                    // Global flag to control exit signal
std::queue<MessageWithTimestamp*> message_queue;  // Global queue to hold messages

//////////////////////////////////////////////////////////////////
// KafkaInstance Class
//////////////////////////////////////////////////////////////////

class KafkaInstance {
public:
    RdKafka::Producer* producer;
    RdKafka::Topic* topic;
    RdKafka::Conf* config;
    string serviceName;
    mutex m;
    std::thread producerThread;         // Producer thread
    atomic<bool> running{true};         // Flag to control the producer thread
    atomic<bool> flag{true};            // Flag to reset startTime
    StringBuffer* TempBuffer;
    StringBuffer* ProduceBuffer;
    Writer<StringBuffer> writer;
    Writer<StringBuffer>* TempWriter = nullptr;
    string *payload;
    int payload_size;
    int cnt = 0;
    int car_num;
    int hz;
    int deadline;
    long size;
    double reliability;
    int TimeCurator;
    int TimeBroker;
    int id;
    thread pollMonitorThread;           // Thread for monitoring poll calls
    atomic<bool> running2{true};        // Flag to control the producer thread
    atomic<int> pollCount{0};           // Count number of poll() calls per second
    atomic<int> totalPollCount{0};      // Total poll count from first poll
    atomic<int> elapsedSeconds{0};      // Number of elapsed seconds
    steady_clock::time_point startPollingTime;  // Time when first poll happened
    bool firstPollHappened = false;     // Flag to track first poll
    chrono::steady_clock::time_point startTime;

    KafkaInstance() : producer(nullptr), topic(nullptr), config(nullptr) {}

    ~KafkaInstance() {
        delete topic;
        delete producer;
        delete config;
    }

    // Initialize the Kafka instance with configuration and topic
    void init(
        const std::string& brokers,
        const std::string& serviceName,
        int car_num,
        int deadline,
        int hz,
        int payload,
        double reliability,
        DeliveryReportCallback& dr_cb,
        int id
    ) {
        this->serviceName = serviceName;
        this->deadline = deadline;
        this->hz = hz;
        this->payload_size = payload;
        this->car_num = car_num;
        this->reliability = reliability;
        this->id = id;

        std::string errstr;

        // Create configuration
        this->config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        if (!config) {
            throw runtime_error("Failed to create RdKafka::Conf");
        }

        // Initialize configuration
        initConf(config, brokers);

        // Create producer
        std::string errStr;
        // Set client.id to serviceName
        if (config->set("client.id", serviceName, errStr) != RdKafka::Conf::CONF_OK) {
            std::cerr << "Failed to set client.id: " << errStr << std::endl;
            exit(1);
        }
        config->set("dr_cb", &dr_cb, errstr);

        this->producer = RdKafka::Producer::create(config, errStr);
        if (!producer) {
            throw std::runtime_error("Failed to create producer: " + errStr);
        }

        // Create topic
        this->topic = RdKafka::Topic::create(producer, serviceName, nullptr, errStr);
        rks[id] = this->producer->c_ptr();
        if (!topic) {
            throw std::runtime_error("Failed to create topic: " + errStr);
        }
    }

    // Helper function to initialize Kafka configuration
    void initConf(RdKafka::Conf* conf, const std::string& brokers) {
        std::string errstr;

        // Set the broker address
        if (conf->set("metadata.broker.list", "192.168.0.1:9092", errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << "Failed to set broker list: " << errstr << std::endl;
            exit(1);
        }
        if (conf->set("acks", "1", errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << "Failed to set acks: " << errstr << std::endl;
            exit(1);
        }
        // Disable Nagle's algorithm
        if (conf->set("socket.nagle.disable", "1", errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << "Failed to disable Nagle algorithm: " << errstr << std::endl;
            exit(1);
        }
        // Set backpressure threshold to 1
        if (conf->set("queue.buffering.backpressure.threshold", "1", errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << "Failed to set queue buffering backpressure threshold: " << errstr << std::endl;
            exit(1);
        }
        if (conf->set("batch.size", to_string(1048576), errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << "Failed to set batch.size: " << errstr << std::endl;
            exit(1);
        }
        // disable linger.ms to 1000
        if (conf->set("linger.ms", to_string(deadline), errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << "Failed to set linger.ms: " << errstr << std::endl;
            exit(1);
        }
        if (conf->set("batch.num.messages", to_string(car_num), errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << "Failed to set batch.size: " << errstr << std::endl;
            exit(1);
        }
    }

    void produceMessage(Message* msg, string* data, Document& parsed_data) {
        {
            int64_t produceTimeMs = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            RdKafka::Headers *headers = RdKafka::Headers::create();
            headers->add("produce_time", std::to_string(produceTimeMs));
            RdKafka::ErrorCode resp = producer->produce(
                topic->name(),
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char*>((*data).c_str()),
                (*data).size(),
                nullptr,
                0,
                produceTimeMs,
                headers,
                nullptr
            );
            int currentPoll = pollCount.load();
            bool isInterval = ((currentPoll % (this->car_num * this->hz)) == 0);
            pollCount.fetch_add(1);
            if (isInterval) {
                int64_t curatorTimeNow = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                for (auto it = parsed_data.MemberBegin(); it != parsed_data.MemberEnd(); ++it) {
                    if (it->name.GetString() == std::string("timestamp")) {
                        int64_t messageTimestamp = std::stoll(it->value.GetString());
                        double curatorDelta = static_cast<double>(curatorTimeNow - messageTimestamp) + 1;
                        TimeC[id] = ceil(max((TimeC[id]),(curatorDelta)));
                        break;
                    }
                }
            }
            BQ.reset_queue(msg->topic);
            producer->poll(0);
        }            
    }
};

KafkaInstance* kafkaInstances[6];

//////////////////////////////////////////////////////////////////
// Global Varibles
//////////////////////////////////////////////////////////////////

RdKafka::Topic* raw_topic;

//////////////////////////////////////////////////////////////////
// MQTTClient Class
//////////////////////////////////////////////////////////////////

class MQTTClient {
public:
    struct mosquitto* mosq;

    MQTTClient() {
        mosquitto_lib_init();
        mosq = mosquitto_new("Consumer", true, this);
        if (!mosq) {
            throw runtime_error("Failed to create mosquitto instance");
        }
        mosquitto_connect_callback_set(mosq, on_connect);
        mosquitto_message_callback_set(mosq, on_message);
    }

    ~MQTTClient() {
        mosquitto_destroy(mosq);
        mosquitto_lib_cleanup();
    }

    void connect(const string& host, int port) {
        int ret = mosquitto_connect(mosq, host.c_str(), port, 30000);
        if (ret) {
            throw runtime_error("Failed to connect to MQTT broker");
        }
        mosquitto_loop_start(mosq);
    }

    static void on_connect(struct mosquitto* mosq, void* obj, int rc) {
        if (rc == 0) {
            mosquitto_subscribe(mosq, nullptr, "#", 0);
        } else {
            cerr << "MQTT Connection failed!" << std::endl;
        }
    }

    static void on_message(struct mosquitto* mosq, void* obj, const struct mosquitto_message* msg) {
        Message* new_msg = new Message();  
        new_msg->topic = strdup(msg->topic);
        new_msg->payload = strdup((char*)msg->payload);
        BQ.put(new_msg->topic, new_msg);
    }
};

MQTTClient client;

//////////////////////////////////////////////////////////////////
// initMqtt
//////////////////////////////////////////////////////////////////

void initMqtt() {
    client.connect("127.0.0.1", 1883);
    cout << "MQTT Connected" << endl;    
}

//////////////////////////////////////////////////////////////////
// parseData
//////////////////////////////////////////////////////////////////

int parseData(Message* msg) {
    string* data = new string(msg->payload);  
    string errstr; // Kafka Error 처리 String
    Document parsed_data;
    string service_name;
    int id = 0;

    if (parsed_data.Parse((*data).c_str()).HasParseError()) {
        cerr << "JSON parse error" << endl;
        return -1;
    }

    // Each service Produce
    for (auto it = parsed_data.MemberBegin(); it != parsed_data.MemberEnd(); ++it) {
        const string key = it->name.GetString();
        if (key[0] > '3' || key[0] < '0') {
            continue;
        }
        id = key[0] - '0';
        break;
    }

    auto instance = kafkaInstances[id];
    instance->produceMessage(msg, data, parsed_data);

    return 0;
}

//////////////////////////////////////////////////////////////////
// process
//////////////////////////////////////////////////////////////////

void process(int& processID) {
    Message* new_msg;
    while (true) {
        new_msg = BQ.take();
        if (new_msg == nullptr) {
            continue;  
        } else {
            parseData(new_msg);
        }
        free(new_msg->topic);    
        free(new_msg->payload);  
        delete new_msg;          
    }
}
//////////////////////////////////////////////////////////////////
// Performance Gauge
//////////////////////////////////////////////////////////////////

void monitorLingerThread() {
    while (running) {
        this_thread::sleep_for(1s);
        if(w_cnt <1){
            w_cnt++;
            continue;
        }
        int size = sizeof(TimeP) / sizeof(TimeP[0]);

        for (int i = 0; i < 4; i++) {
            double oldLinger = batch_ms[i];
            // new linger.ms = deadline - UL - DL - TimeC(Time in Curator) - TimeP(Time for Produce) - TimeP(Time for Fetch)
            int newLinger = max(static_cast<int>(floor(
                static_cast<double>(service_deadline[i]) - 6 - *max_element(TimeP,TimeP+4) - *max_element(TimeP,TimeP+4) - *max_element(TimeC,TimeC+4)
            )),5);
            rd_kafka_update(rks[i], newLinger,car_num);
            batch_ms[i] = newLinger;
            TimeC[i] = 0;
            TimeP[i] = 0;
        }
    }
}




//////////////////////////////////////////////////////////////////
// main
//////////////////////////////////////////////////////////////////

int main(int argc, char* argv[]) {
    if (argc != 4) {
        cerr << "Usage: " << argv[0] << " num_cars second num_processors" << endl;
        return 1;
    }

    car_num = stoi(argv[1]);
    int second = stoi(argv[2]);        
    int numProcessors = stoi(argv[3]);
    string brokers = "192.168.0.1:9092";

    for(int i = 1; i <= car_num; i++) {
        string topic = "Car" + to_string(i);
        BQ.New_Queue(topic);
    }

    for (size_t i = 0; i < 4; i++) {
        cb_list.push_back(new DeliveryReportCallback(car_num, service_hz[i], i));
        kafkaInstances[i] = new KafkaInstance();
        kafkaInstances[i]->init(
            brokers,
            service_names[i],
            car_num,
            service_deadline[i],
            service_hz[i],
            service_payload[i],
            service_reliability[i],
            ref(*cb_list[i]),
            i
        );
    }

    initMqtt();
    vector<thread> processors;                  
    vector<int> processorCounters(numProcessors);

    for (int i = 0; i < numProcessors; ++i) {
        processors.emplace_back(process, ref(processorCounters[i]));
    }
    thread monitorThread(monitorLingerThread);

    for (auto& processor : processors) {
        processor.join();
    }
    cout << "Processor End" << endl;
    return 0;
}
