#include </home/hss544/json.hpp>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <librdkafka/rdkafkacpp.h>
#include </home/hss544/librdkafka/src-cpp/rdkafkacpp_int.h>
#include <cstring>
#include <map>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <signal.h>
#include <chrono>
#include <iomanip>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fstream>
#include <csignal>
#include <cstdlib>

using namespace std;
using json = nlohmann::json;
// Function to handle each Kafka consumer thread for a single vehicle
struct MessageWithTimestamp {
    RdKafka::Message* message;
    int64_t arrival_time;
    string service_name; // To identify which service this message belongs to
    string * msg;
    MessageWithTimestamp(string* msg, int64_t time, const string& service)
        : msg(msg), arrival_time(time), service_name(service) {}
};
const int SENSOR_INFO_INTERVAL = 100;
const int INFORMATION_SHARING_INTERVAL = 100;
const int PLATOONING_LOWEST_INTERVAL = 25;
const int PLATOONING_LOWER_INTERVAL = 20;
const int PLATOONING_HIGH_INTERVAL = 10;
const int COLLISION_AVOIDANCE_INTERVAL = 10;

// Global variables
int cnt = 0;
int numPartitions = 1;
int service_num = 20;
int car_num = 0;
int p_cnt = 0;
bool flag = true;
int seconds = 0;
int max_cnt=0;
int sec_cnt=0;
mutex m_cnt;
mutex queue_mutex;
condition_variable cv;
bool running = true;  // Global flag to control exit signal
queue<MessageWithTimestamp*> message_queue;  // Global queue to hold messages with timestamps
bool warm_flag=false;
mutex m_running;
struct ServiceStats {
    int success = 0;
    int fail = 0;
    int gap =0;
    int warm_success=1;
    int warm_fail=0;
    int warm_gap=0;
};
vector<RdKafka::KafkaConsumer*> allConsumer;
map<string, ServiceStats> service_stats;
int loop_cnt=0;



// Function to simulate one vehicle using 4 consumers (one per service)
void simulateVehicleWithMultipleConsumers(const string& brokers, string& topic, const string& groupId, int vehicleId) {
    string errstr;
    RdKafka::KafkaConsumer* consumer;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    string clientId = "vehicle_" + to_string(vehicleId) + "_" + topic;
    if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("group.id", groupId, errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("auto.offset.reset", "latest", errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("enable.auto.commit", "false", errstr) ||
        conf->set("client.id", clientId, errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("session.timeout.ms", "6000", errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("heartbeat.interval.ms", "2000", errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("max.poll.interval.ms", "2000000", errstr) != RdKafka::Conf::CONF_OK) {
        cerr << "Failed to set Kafka configuration for vehicle " << vehicleId << " topic " << topic << ": " << errstr << endl;
        delete conf;
        return;
    }
    
    // Create consumer for the topic
    consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer) {
        cerr << "Failed to create consumer for vehicle " << vehicleId << " topic " << topic << ": " << errstr << endl;
        delete conf;
        return;
    }
    delete conf;
    // Subscribe to the topic
    RdKafka::ErrorCode resp = consumer->subscribe({topic});
    if (resp != RdKafka::ERR_NO_ERROR) {
        cerr << "Vehicle " << vehicleId << " failed to subscribe to topic " << topic << ": " << RdKafka::err2str(resp) << endl;
        delete consumer;
        return;
    }        
    std::cout << "Vehicle " << vehicleId << " subscribed to topic " << topic <<endl;    
    // Consume messages from all consumers
    while (true) {
        RdKafka::Message* msg = consumer->consume(100); // 0ms timeout
        delete msg;
    }
}
int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " num_cars seconds" << endl;
        return 1;
    }
    car_num = atoi(argv[1]);
    seconds = atoi(argv[2]);
    max_cnt = car_num * seconds * (10 + 10 + 33 + 50);
    sec_cnt = car_num * (10 + 10 + 33 + 50);
    string brokers = "192.168.0.1:9092";
    string groupId = "Remote_Vehicle";
    // Define Kafka topics for all services
    vector<string> topics = {
        "Sensor_Sharing",
        "Information_Sharing",
        "Platooning_Lowest",
        "Platooning_Lower"
    };
    // Create threads for each vehicle
    vector<thread> vehicleThreads;
    for (int i = 4; i < car_num; ++i) {
        vehicleThreads.emplace_back(simulateVehicleWithMultipleConsumers, brokers, ref(topics[i%4]), groupId+to_string(i+1), i + 1);
    }
    // Wait for all vehicle threads to complete
    for (auto& t : vehicleThreads) {
        if (t.joinable()) {
            t.join();
        }
    }
    return 0;
}
