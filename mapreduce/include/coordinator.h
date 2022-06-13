#ifndef MAPREDUCE_COORDINATOR_H
#define MAPREDUCE_COORDINATOR_H

#include <map>
#include <string>
#include <vector>
#include <mutex>
#include <atomic>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "rpc.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using namespace mr;

class Coordinator final : public Rpc::Service {
public:
    enum WorkerStatus {
        Free,
        Busy,
        Timeout,
    };
    static Coordinator* make_coordinator(const std::vector<std::string>& files, uint32_t nreduce);
    bool done();
    void run();
public:
    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;
    ~Coordinator() = default;
public:
    // rpc
    Status IsDone(ServerContext* context, const IsDoneArgs* args, IsDoneReply* reply);
    Status AskTask(ServerContext* context, const AskTaskArgs* args, AskTaskReply* reply);
    Status ReportTask(ServerContext* context, const ReportTaskArgs* args, ReportTaskReply* reply);
    Status AmIOK(ServerContext* context, const AmIOKArgs* args, AmIOKReply* reply);
private:
    Coordinator() = default;
private:
    std::mutex _mu;
    std::atomic<uint32_t> _workers{};
    bool _mapTaskIsDone{};
    bool _reduceTaskIsDone{};
    uint32_t _remainMapTasks{};
    uint32_t _remainReduceTasks{};
    std::map<uint32_t, WorkerStatus> _ws; // worker's status 
    uint32_t _nreduce{}; // 需要多少个 reduce task
    
    // map task
    std::vector<std::string> _files; // 需要处理的所有文件
    std::vector<std::string> _intermediateFiles; // worker 把 map task 的中间结果放到文件中，所以需要记录哪些文件存储了中间结果
    std::map<uint32_t, std::string> _workerToMapTask; // 哪个 worker 正在做哪个 map task
    std::map<std::string, uint32_t> _mapTasks; // =0 表示未分配，=1 表示已分配未执行完，=2 表示已经处理完了

    // reduce task
    std::map<uint32_t, uint32_t> _workerToReduceTask; // 哪个 worker 正在做哪个 reduce task
    std::map<uint32_t, uint32_t> _reduceTasks; // =0 表示未分配，=1 表示已分配未执行完，=2 表示已经处理完了

    // crash
    std::map<uint32_t, uint32_t> _timers; // worker i 已经 j 秒没有和 Coordinator 通信了；规定 8s 没通信则视为 down
};

#endif 
