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
#include <dirent.h>
#include <signal.h>
#include <atomic>
#include<iostream>
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
const int SENSOR_INFO_PAYLOAD = 1600;
const int INFORMATION_SHARING_PAYLOAD = 6500;
const int PLATOONING_LOWEST_PAYLOAD = 400;
const int PLATOONING_LOWER_PAYLOAD = 6500;
bool log_flag=false;
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
std::atomic<bool> stopAll(false);

mutex m_cnt;
mutex queue_mutex;
condition_variable cv;
bool running = true;  // Global flag to control exit signal
// queue<MessageWithTimestamp*> message_queue;  // Global queue to hold messages with timestamps
queue<MessageWithTimestamp*> sensor_queue;  // Global queue to hold messages with timestamps
queue<MessageWithTimestamp*> info_queue;  // Global queue to hold messages with timestamps
queue<MessageWithTimestamp*> lower_queue;  // Global queue to hold messages with timestamps
queue<MessageWithTimestamp*> lowest_queue;  // Global queue to hold messages with timestamps
std::mutex statsMutex;    // Protects service_stats
std::mutex consoleMutex;
mutex m_running;
mutex m_print;
int global_cnt=0;
struct ServiceStats {
    int success = 0;
    int fail    = 0;
    long double gap     = 0;

    int warm_success = 0;
    int warm_fail    = 0;
    int warm_gap     = 0;
    // For throughput (based on production timestamp)
    bool    isFirstTimestampSet = false;
    int64_t firstTimestamp      = 0;
    int64_t lastTimestamp       = 0;
    int     payload             = 0;
    int     total_paylod        = 0;
    double throughput;
    int cnt;
    int loop;
    double ops;
    std::vector<int64_t> gapValues;

};
vector<RdKafka::KafkaConsumer*> allConsumer;
map<string, ServiceStats> service_stats;
int loop_cnt=0;

ofstream outFile3("Warm_Gap.txt");
int64_t timespec_to_milliseconds(const struct timespec ts) {
    return (int64_t)(ts.tv_sec) * 1000 + (ts.tv_nsec + 500000) / 1000000;
}
void clearScreen() {
    std::cout << "\033[2J\033[H"; // Clear screen and move cursor to the top-left
}
// Helper function to display the progress bar
void displayProgressBar(int current, int total) {
    const int barWidth = 50;  
    float progress = (total == 0) ? 0.0f : static_cast<float>(current) / total;
    int pos = static_cast<int>(barWidth * progress);

    std::lock_guard<std::mutex> lock(consoleMutex); // to avoid garbled output
    std::cout << "[";
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos)       std::cout << "=";
        else if (i == pos) std::cout << ">";
        else               std::cout << " ";
    }
    std::cout << "] "
              << std::setw(6) << std::fixed << std::setprecision(2)
              << (progress * 100) << "%\r";
    std::cout.flush();
}

// Function to log service statistics
void logServiceStats() {
    using std::setw;
    using std::left;
    using std::fixed;
    using std::setprecision;

    // 헤더 출력
    std::cout << left
              << setw(18) << "Service"
              << setw(12) << "Rel(%)"
              << setw(12) << "AVG Gap"
              << setw(12) << "OP/S"
              << setw(12) << "Total Ops"
            //   << setw(12) << "90% Gap"
            //   << setw(12) << "99% Gap"
              
              
            //   << setw(12) << "Throughput"
              << "\n"
              << std::string(90, '-') << "\n";
    // stats를 안전하게 접근하기 위해 lock
    std::lock_guard<std::mutex> lock(statsMutex);

    for (auto &kv : service_stats) {
        auto &service = kv.first;
        auto &cnt     = kv.second; // ServiceStats 참조

        int totalMessages = static_cast<int>(cnt.success + cnt.fail);

        double reliability = (totalMessages > 0)
            ? (100.0 * cnt.success / static_cast<double>(totalMessages))
            : 0.0;

        long double avgGap = (totalMessages > 0)
            ? static_cast<long double>(cnt.gap) / totalMessages
            : 0.0;

        // ============== 퍼센타일 계산 로직 ==============
        // 벡터를 복사/정렬
        std::vector<int64_t> sortedGaps = cnt.gapValues;
        if (!sortedGaps.empty()) {
            std::sort(sortedGaps.begin(), sortedGaps.end());

            // 90% 인덱스(0-based)
            size_t idx90 = static_cast<size_t>(
                std::floor(0.9 * sortedGaps.size())
            );
            if(idx90 >= sortedGaps.size()) {
                idx90 = sortedGaps.size() - 1; 
            }
            int64_t p90 = sortedGaps[idx90];

            // 99% 인덱스
            size_t idx99 = static_cast<size_t>(
                std::floor(0.99 * sortedGaps.size())
            );
            if(idx99 >= sortedGaps.size()) {
                idx99 = sortedGaps.size() - 1; 
            }
            int64_t p99 = sortedGaps[idx99];

            // 출력할 때 표로 같이 표시
            std::cout << left
                      << setw(18) << service
                      << setw(12) << fixed << setprecision(2) << reliability
                      << setw(12) << fixed << setprecision(2) << avgGap
                      << setw(12) << fixed << setprecision(2) << cnt.ops
                      << setw(12) << totalMessages
                    //   << setw(12) << p90
                    //   << setw(12) << p99
                      
                      
                    //   << setw(12) << fixed << setprecision(2) << cnt.throughput
                      << "\n";
        }
        else {
            // gapValues가 비어있는 경우 처리
            std::cout << left
                      << setw(18) << service
                      << setw(12) << fixed << setprecision(2) << reliability
                      << setw(12) << fixed << setprecision(2) << avgGap
                      << setw(12) << 0
                      << setw(12) << 0
                      << setw(12) << totalMessages
                      << setw(12) << fixed << setprecision(2) << cnt.ops
                      << setw(12) << fixed << setprecision(2) << cnt.throughput
                      << "\n";
        }
        // ================================================
    }
    std::cout << std::string(90, '-') << "\n";
}
void logServiceStats_File(std::ofstream &ofs) {
    using std::setw;
    using std::left;
    using std::fixed;
    using std::setprecision;

    // Print header to ofs
    ofs << left
        << setw(18) << "Service"
        << setw(12) << "Rel(%)"
        << setw(12) << "AVG Gap"
        << setw(12) << "OP/S"
        << setw(12) << "Total Ops"
        // << setw(12) << "90% Gap"
        // << setw(12) << "99% Gap"
        // << setw(12) << "Throughput"
        << "\n"
        << std::string(90, '-') << "\n";

    // Lock for thread safety when accessing service_stats
    std::lock_guard<std::mutex> lock(statsMutex);

    for (auto &kv : service_stats) {
        const auto &service      = kv.first;
        auto       &stats        = kv.second; // ServiceStats reference

        int totalMessages = stats.success + stats.fail;

        double reliability = (totalMessages > 0)
            ? (100.0 * stats.success / static_cast<double>(totalMessages))
            : 0.0;

        long double avgGap = (totalMessages > 0)
            ? static_cast<long double>(stats.gap) / totalMessages
            : 0.0;

        // ============== Percentile calculation ==============
        std::vector<int64_t> sortedGaps = stats.gapValues;
        if (!sortedGaps.empty()) {
            std::sort(sortedGaps.begin(), sortedGaps.end());

            // 90% index
            size_t idx90 = static_cast<size_t>(std::floor(0.9 * sortedGaps.size()));
            if (idx90 >= sortedGaps.size()) {
                idx90 = sortedGaps.size() - 1;
            }
            int64_t p90 = sortedGaps[idx90];

            // 99% index
            size_t idx99 = static_cast<size_t>(std::floor(0.99 * sortedGaps.size()));
            if (idx99 >= sortedGaps.size()) {
                idx99 = sortedGaps.size() - 1;
            }
            int64_t p99 = sortedGaps[idx99];

            // Write stats to file
            ofs << left
                << setw(18) << service
                << setw(12) << fixed << setprecision(2) << reliability
                << setw(12) << fixed << setprecision(2) << avgGap
                << setw(12) << fixed << setprecision(2) << stats.ops
                << setw(12) << totalMessages
                // << setw(12) << p90
                // << setw(12) << p99
                // << setw(12) << fixed << setprecision(2) << stats.throughput
                << "\n";
        } else {
            // If gapValues is empty
            ofs << left
                << setw(18) << service
                << setw(12) << fixed << setprecision(2) << reliability
                << setw(12) << fixed << setprecision(2) << avgGap
                << setw(12) << 0
                << setw(12) << 0
                << setw(12) << totalMessages
                // If you have throughput or other fields:
                // << setw(12) << fixed << setprecision(2) << stats.ops
                // << setw(12) << fixed << setprecision(2) << stats.throughput
                << "\n";
        }
        // ====================================================
    }
    ofs << std::string(90, '-') << "\n";
}

bool processMessage(const json &parsed_json,
                    int64_t arrival_time,
                    ofstream &outFile,
                    mutex &mu){
    static const int TIME_ADJUSTMENT = 3;
    int64_t message_timestamp;
    // Extract message production time
    if (!parsed_json.contains("timestamp")) {
        return false;
    }

        
    try {
        message_timestamp = stoll(parsed_json.at("timestamp").get<std::string>());
    } catch (...) {
        std::cerr << "Invalid message timestamp.\n";
        return false;
    }

    // Identify service & interval
    int    interval     = 0;
    string service_name;
    if (parsed_json.contains("0Sensor_information_sharing")) {
        interval     = SENSOR_INFO_INTERVAL;
        service_name = "Sensor";
    }
    else if (parsed_json.contains("1Information_sharing_for_automated_driving")) {
        interval     = INFORMATION_SHARING_INTERVAL;
        service_name = "Information";
    }
    else if (parsed_json.contains("2Cooperative_driving_for_vehicle_platooning_lowest")) {
        interval     = PLATOONING_LOWEST_INTERVAL;
        service_name = "Lowest";
    }
    else if (parsed_json.contains("3Cooperative_driving_for_vehicle_platooning_lower")) {
        interval     = PLATOONING_LOWER_INTERVAL;
        service_name = "Lower";
    }
    else if (parsed_json.contains("Cooperative_driving_for_vehicle_platooning_high")) {
        interval     = PLATOONING_HIGH_INTERVAL;
        service_name = "Platooning_High";
    }
    else if (parsed_json.contains("Cooperative_collision_avoidance")) {
        interval     = COLLISION_AVOIDANCE_INTERVAL;
        service_name = "Collision_Avoidance";
    } else {
        return false;
    }

    // Calculate gap
    int64_t gap = arrival_time - message_timestamp + TIME_ADJUSTMENT;
    
    // Log to file (optional)

    // Update stats
    auto &stats = service_stats[service_name];
    // stats.total_paylod+=parsed_json.size();
    std::lock_guard<std::mutex> lock(mu);
    outFile << "Service: " << service_name
        << " | Produce Time: " << message_timestamp
        << " | Arrival Time: " << arrival_time
        << " | Gap: " << gap
        << "\n";

    if (gap > interval) {
        stats.fail++;
    } else {
        stats.success++;
    }
    stats.gap += gap;
    stats.cnt++;
    stats.gapValues.push_back(gap);

    // Throughput timestamps
    if (!stats.isFirstTimestampSet) {
        stats.isFirstTimestampSet = true;
        stats.firstTimestamp      = arrival_time;
        stats.lastTimestamp       = arrival_time;
    } else if (arrival_time > stats.lastTimestamp) {
        stats.lastTimestamp = arrival_time;
    }
    if(stats.lastTimestamp - stats.firstTimestamp>1000){
        stats.loop++;
        stats.throughput=static_cast<double>(stats.cnt)*stats.payload / 1024 / 1024 / stats.loop;
        stats.ops =         static_cast<double>(stats.cnt) / stats.loop;
        stats.firstTimestamp= stats.lastTimestamp;
        // stats.cnt=0;
        log_flag=true;
        stats.isFirstTimestampSet = false;
    }
    return true;
}

void simulateVehicleWithMultipleConsumers_Log(const string& brokers, string topic, const string& groupId, int vehicleId,queue<MessageWithTimestamp*>* queue,int max_cnt) {
    string errstr;
    vector<RdKafka::KafkaConsumer*> consumers;
    int cnt=0;
    RdKafka::KafkaConsumer* consumer;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    string clientId = "vehicle_" + to_string(vehicleId) + "_" + topic;
    // Configure Kafka consumer
    if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("group.id", groupId, errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("auto.offset.reset", "latest", errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("enable.auto.commit", "false", errstr) ||
        conf->set("client.id", clientId, errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("session.timeout.ms", "6000", errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("heartbeat.interval.ms", "2000", errstr) != RdKafka::Conf::CONF_OK ||
        conf->set("max.poll.interval.ms", "20000", errstr) != RdKafka::Conf::CONF_OK) {
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
    std::cout << "Vehicle " << vehicleId << " subscribed to topic " << topic << endl;
    // Consume messages from all consumers
    while (!stopAll&&queue->size() <= max_cnt) {
        // Poll messages for each consumer
        RdKafka::Message* msg = consumer->consume(0); // 0ms timeout
        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            auto a = static_cast<RdKafka::MessageImpl*>(msg)->rkmessage_;
            int64_t timestamp = timespec_to_milliseconds(a->ts);
            string *payload = new string();
            *payload = reinterpret_cast<const char*>(msg->payload());
            MessageWithTimestamp* messageWithTime = new MessageWithTimestamp(payload, timestamp, msg->topic_name());
            queue->push(messageWithTime);
        }
        delete msg;
    }
    lock_guard<mutex> lock(m_print);
    std::cout << topic <<" Total Cnt messages arrived:"<<queue->size()<<"Expected: "<<max_cnt <<" Ratio: "<<static_cast<double>(queue->size()) / max_cnt * 100<<"%"
 << endl;
}
// Worker thread function
void workerThread(std::queue<MessageWithTimestamp*>& message_queue,
                  std::mutex& queueMutex,
                  std::atomic<int>& processedCount,
                  int totalMessages,
                  std::ofstream &outFile,
                  std::mutex& mu)
{
    while (true) {
        // 1) Lock queue and pop
        displayProgressBar(processedCount.load(), totalMessages);
        MessageWithTimestamp* msg = nullptr;
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (message_queue.empty()) {
                // No more messages
                break;
            }
            msg = message_queue.front();
            message_queue.pop();
        }

        // 2) Process the message
        if (msg && msg->msg) {
            try {
                json parsed_json = json::parse(*(msg->msg));
                processMessage(parsed_json, msg->arrival_time, outFile, mu);
            }
            catch (json::parse_error& e) {
                std::cerr << "JSON parse error: " << e.what() << std::endl;
            }
            delete msg->msg;
            delete msg;
        }

        // 3) Update counters
        processedCount++;
        global_cnt++;

        // 4) Optional: update progress bar

        // 5) Optionally, log stats if needed
        if (log_flag) {
            log_flag = false;
        }
    }
}

// Main parallel function to process all messages
void processMessagesParallel(std::ofstream& outFile, std::ofstream& outFile2,
                             std::queue<MessageWithTimestamp*>& message_queue,
                             int threadCount = 4,
                             string service="")
{
    int total_messages = static_cast<int>(message_queue.size());
    // For progress bar
    std::atomic<int> processedCount{0};
    // Create worker threads
    std::mutex queueMutex; // Protects the message_queue
    std::vector<std::thread> workers;
    workers.reserve(threadCount);
    std::mutex mu; // Protects the message_queue

    for (int i = 0; i < threadCount; i++) {
        workers.emplace_back(workerThread,
                            std::ref(message_queue),
                            std::ref(queueMutex),
                            std::ref(processedCount),
                            total_messages,
                            std::ref(outFile),
                            std::ref(mu));
    }
    // Wait for all threads to finish
    for (auto &t : workers) {
        t.join();
    }
    std::cout << std::endl;  // End progress bar line
}
std::string getTimestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm* tm_info = std::localtime(&now_time);
    
    std::ostringstream oss;
    oss << std::put_time(tm_info, "%Y%m%d_%H%M%S");  // Format: YYYYMMDD_HHMMSS
    return oss.str();
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        cerr << "Usage: " << argv[0] << " num_cars seconds threads" << endl;
        return 1;
    }
    car_num = atoi(argv[1]);
    seconds = atoi(argv[2]);
    int threads = atoi(argv[3]);
    max_cnt = car_num * seconds * (10 + 10 + 33 + 50);
    sec_cnt = car_num * (10 + 10 + 33 + 50);
    int temp_cnt = car_num * seconds;
    string brokers = "192.168.0.1:9092";
    string groupId = "vehicle";
    service_stats["Sensor"].payload=SENSOR_INFO_PAYLOAD;
    service_stats["Information"].payload=INFORMATION_SHARING_PAYLOAD;
    service_stats["Lowest"].payload=PLATOONING_LOWEST_PAYLOAD;
    service_stats["Lower"].payload=PLATOONING_LOWER_PAYLOAD;
    vector<thread> vehicleThreads;
    vehicleThreads.emplace_back(simulateVehicleWithMultipleConsumers_Log, brokers, "Sensor_Sharing", "Test_Sensor",  1,&sensor_queue,temp_cnt*10);
    vehicleThreads.emplace_back(simulateVehicleWithMultipleConsumers_Log, brokers, "Information_Sharing", "Test_Info",  1,&info_queue,temp_cnt*10);
    vehicleThreads.emplace_back(simulateVehicleWithMultipleConsumers_Log, brokers, "Platooning_Lowest", "Test_Lowest",  1,&lowest_queue,temp_cnt*33);
    vehicleThreads.emplace_back(simulateVehicleWithMultipleConsumers_Log, brokers, "Platooning_Lower", "Test_Low",  1,&lower_queue,temp_cnt*50);
    std::string timestamp = getTimestamp();
    std::string gapFileName = "Thor_Gap_" + std::to_string(car_num) + "_" + std::to_string(threads) + "_" + timestamp + ".txt";
    std::string ratioFileName = "Thor_Ratio_" + std::to_string(car_num) + "_" + std::to_string(threads) + "_" + timestamp + ".txt";
    std::ofstream outFile(gapFileName);
    std::ofstream outFile2(ratioFileName);
    this_thread::sleep_for(chrono::milliseconds(1000));
    cout<<"Multi Ready"<<endl;
    cout<<"Run Vehicles"<<endl;
    // ----------------------------------------------------------------------
    {   
    cout<<"Wating 70 seconds..."<<endl;
    this_thread::sleep_for(chrono::milliseconds(seconds*1000+10000));
    stopAll = true;
    // Wait for all vehicle threads to complete
    for (auto& t : vehicleThreads) {
        if (t.joinable()) {
            t.join();
        }
    }
    cout<<"Process Start"<<endl;
    processMessagesParallel(outFile, outFile2,lowest_queue, 16,"Lowest");
    processMessagesParallel(outFile, outFile2,lower_queue,  16,"Low");
    processMessagesParallel(outFile, outFile2,sensor_queue, 16,"Sensor");
    processMessagesParallel(outFile, outFile2,info_queue,   16,"Info");
    clearScreen();
    logServiceStats();
    logServiceStats_File(outFile2);
    outFile.close();
    outFile2.close();
        cout<<"Kill Vehicles"<<endl;
    // Final reporting
    std::cout << "Processing complete. Final statistics written to:\n";
    std::cout << "  - " << gapFileName << std::endl;
    std::cout << "  - " << ratioFileName << std::endl;
    return 0;
}
