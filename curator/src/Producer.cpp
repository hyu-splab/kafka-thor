#include <mosquitto.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <chrono>
#include <thread>
#include <numeric>
#include <iomanip>
#include <sstream>
#include <mutex>
#include <atomic>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <ctime>     // time()
#include <random>  // std::random_device, std::mt19937, std::uniform_int_distribution

using namespace std;

// Global variables
std::atomic<bool> start_flag(false);  // Atomic flag to control thread start
int car_num = 0;                      // Number of cars (MQTT clients)
int tries = 0;                        // Number of publishing attempts
mutex m_num;                          // Mutex to control access to shared variables
int m_cnt=0;
// Constants for time intervals (in milliseconds)
const int SENSOR_INFO_INTERVAL = 100;
const int PLATOONING_LOWER_INTERVAL = 20;
const int PLATOONING_LOWEST_HIGH_INTERVAL = 30;
const int COLLISION_AVOIDANCE_INTERVAL = 10;
// Payload values (payload sizes)
const int SENSOR_INFO_Payload = 1600;
const int AUTO_DRIVE_Payload = 6500;
const int PLATOONING_LOWER_Payload = 6500;
const int PLATOONING_LOWEST_Payload = 400;
int seconds = 0;
int max_cnt=0;
int sec_cnt=0;

struct ServiceCounters {
    int sensor_info_sharing = 0;
    int auto_driving_info = 0;
    int platooning_lower = 0;
    int platooning_lowest = 0;
    int platooning_high = 0;
    int collision_avoidance = 0;
};

string current_time() {
    auto now = chrono::system_clock::now();
    auto now_time_t = chrono::system_clock::to_time_t(now);
    auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

    stringstream ss;
    ss << put_time(localtime(&now_time_t), "%Y-%m-%d %H:%M:%S");
    ss << '.' << setfill('0') << setw(3) << now_ms.count(); // Append milliseconds
    return ss.str();
}

void print_time(string str,int cnt){
    auto now = std::chrono::system_clock::now();
    auto nowAsTimeT = std::chrono::system_clock::to_time_t(now);
    auto nowAsTm = *std::localtime(&nowAsTimeT);
    auto nowMs = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto value2 = nowMs.time_since_epoch();
    long duration = value2.count();
    long milliseconds = duration % 1000; // Extract milliseconds
    cout <<str<<": " <<car_num<<":"<<""<< cnt << ": " << std::put_time(&nowAsTm, "%H:%M:%S.") << setw(3) << setfill('0') << milliseconds <<" " <<endl;
}
string generate_car_data(const string& service, int payload_size, std::chrono::system_clock::time_point timevalue, string str) {
    // Pre-calculate parts that do not change with the payload
    int64_t timestamp = chrono::duration_cast<chrono::milliseconds>(timevalue.time_since_epoch()).count();
    string timestamp_str = to_string(timestamp);
    
    size_t fixed_size = 52 + 2 * service.size() + timestamp_str.size() + str.size();
    
    // Determine the size of large_value so that the final string is exactly payload_size characters.
    int large_value_size = payload_size - fixed_size;
    // If payload_size is too small to accommodate the fixed parts, we set large_value to empty.
    if (large_value_size < 0) {
        large_value_size = 0;
    }
    string large_value(large_value_size, 'x');  // Payload filled with 'x' characters

    // Build the JSON string exactly as before
    string services_data = "{\n";
    services_data += "\"service_name\":\"" + service + "\",";
    string service_data = "\"" + service + "\":\"";
    services_data += service_data + large_value + "\"";
    services_data += ",\"timestamp\":\"" + timestamp_str + "\"";
    services_data += ",\"cnt\":\"" + str + "\" \n}";
    return services_data;
}

// Mosquitto connection callback
void on_connect(struct mosquitto *mosq, void *obj, int reason_code) {
    if (reason_code != 0) {
        cerr << "Connection failed: " << reason_code << endl;
    }
}

void publish_message(mosquitto* mosq, const string &topic_name, const string &service_name, int payload_size,string str) {
    string car_data = generate_car_data(service_name, payload_size, chrono::system_clock::now(),str);
    int resp = mosquitto_publish(mosq, nullptr, topic_name.c_str(), car_data.length(), car_data.c_str(), 1, false);
    if (resp != 0) {
        cerr << "Error publishing " << service_name << " to topic " << topic_name << endl;
    }
    m_num.lock();
    m_cnt++;
     if(m_cnt%(sec_cnt)==0){
        // print_time("Produce",m_cnt/(sec_cnt));
     }
    m_num.unlock();
}

void publish_for_car(int car_id, int tries,int start_time) {
    // Create a Mosquitto client for each car thread
    // std::srand(static_cast<unsigned>(std::time(nullptr)));
    int elapsed_time = start_time;

    string client_id = "Car" + to_string(car_id);
    mosquitto *mosq = mosquitto_new(client_id.c_str(), true, nullptr);
        // Check if the Mosquitto instance creation failed
    if (!mosq) {
        cerr << "Failed to create Mosquitto instance for Car" << car_id << endl;
        return;
    }
    // Set connection callback
    mosquitto_connect_callback_set(mosq, on_connect);
    // Connect to the local MQTT broker (assuming it’s running on localhost:1883)
    int ret = mosquitto_connect(mosq, "127.0.0.1", 1883, 60);
    if (ret) {
        cerr << "Could not connect to Broker for Car " << car_id << " with return code " << ret << endl;
        mosquitto_destroy(mosq);
        return;
    }
    // Wait for the start flag to be set
    while (!start_flag.load(memory_order_acquire)) {
        // elapsed_time+=10;
        this_thread::yield();
    }
    // Start Mosquitto loop in the background
    mosquitto_loop_start(mosq);
    std::random_device rd;

    int cnt = 0;
    int total_time = tries*1000+start_time;
    string str;
    string topic_name = "Car" + to_string(car_id); // Topic name for each car
    while (elapsed_time <= total_time) {
        // Every 100ms: Sensor Information Sharing & Automated Driving Information
        // Every 20ms: Cooperative Driving for Platooning (Lower)
        if (elapsed_time % 20 == 0) {
            publish_message(mosq, topic_name, "3Cooperative_driving_for_vehicle_platooning_lower", PLATOONING_LOWER_Payload,str);
        }
        // Every 30ms: Cooperative Driving for Platooning (Lowest & Highest)
        if (elapsed_time % 30 == 0) {
            publish_message(mosq, topic_name, "2Cooperative_driving_for_vehicle_platooning_lowest", PLATOONING_LOWEST_Payload,str);
        }
        if (elapsed_time % 100 == 0) {
           publish_message(mosq, topic_name, "0Sensor_information_sharing", SENSOR_INFO_Payload,str);
           publish_message(mosq, topic_name, "1Information_sharing_for_automated_driving", AUTO_DRIVE_Payload,str);
        }
        elapsed_time += 5;  // Increment time by 10ms
        this_thread::sleep_for(chrono::milliseconds(5));
    }
    // Stop Mosquitto loop and clean up
    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    return;
}
void setup_mosquitto() {
    mosquitto_lib_init();  // Initialize the Mosquitto library
}
void cleanup_mosquitto() {
    mosquitto_lib_cleanup();  // Clean up the Mosquitto library
}
int main(int argc, char *argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " car_num seconds" << endl;
        return 1;
    }
    car_num = atoi(argv[1]);
    seconds = atoi(argv[2]);
    max_cnt = car_num * seconds * (10 + 10 + 33 + 50);
    sec_cnt = car_num * (10 + 10 + 33 + 50);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 19);

    // Initialize Mosquitto
    setup_mosquitto();
    vector<thread> threads;
    // Launch threads for each car
    for (int i = 1; i <= car_num; ++i) {
        int randomMultipleOf5 = dist(gen) * 5;
        threads.emplace_back(publish_for_car, i, seconds,randomMultipleOf5);
        this_thread::sleep_for(chrono::milliseconds(10));

    }
    cout << "[" << current_time() << "] All threads are ready. Starting publishing for " << car_num << " cars." << endl;
    cout << "[" << current_time() << "] Sleep 10s to wait Kakfa" << endl;
    this_thread::sleep_for(chrono::milliseconds(5000));
    // Set the flag to allow threads to start publishing
    start_flag.store(true, memory_order_release);
    // Wait for all threads to complete
    for (auto &t : threads) {
        t.join();
    }
    cleanup_mosquitto();
    return 0;
}
