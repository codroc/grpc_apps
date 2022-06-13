#ifndef MAPREDUCE_WORKER_H
#define MAPREDUCE_WORKER_H

#include <memory>
#include <set>
#include <string>
#include <functional>

#include <rapidjson/document.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>
#include <fstream>

#include "rpc.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using mr::Rpc;
using mr::IsDoneArgs;
using mr::IsDoneReply;
using mr::AskTaskArgs;
using mr::AskTaskReply;
using mr::ReportTaskArgs;
using mr::ReportTaskReply;
using mr::AmIOKArgs;
using mr::AmIOKReply;

struct KeyValue {
    KeyValue(const std::string& key, const std::string& value)
        : Key(key)
        , Value(value)
    {}
    std::string Key;
    std::string Value;

    template <class T>
    void json_dump(rapidjson::Writer<T>&) const;
};

using MapFunc = std::function<std::vector<KeyValue>(std::string, std::string)>;
using ReduceFunc = std::function<std::string(std::vector<std::string>)>;

std::pair<MapFunc, ReduceFunc> LoadPlugin();

class Worker {
public:
    static Worker* make_worker();
    void work();
public:
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;
private:
    Worker();
    bool done(); // need to ask coordinator weather it is done.

    // 询问 coordinator 我的工作它是否认可
    bool is_ok();
    bool has_task(const AskTaskReply& reply);
    bool is_map_task(const AskTaskReply& reply);
    AskTaskReply ask_task();
    void do_map_task(const AskTaskReply& reply, MapFunc mapf);
    void do_reduce_task(const AskTaskReply& reply, ReduceFunc reducef);
    // 向 coordinator 汇报完成的任务
    // 如果是 map task，那么就是生成的中间文件
    // 如果是 reduce task，那么就是 reduce number
    ReportTaskReply report_task(const std::set<std::string>& intermediate_files, int reduce_number);
private:
    int _workerId{};
    uint32_t _times{};
    uint32_t _nreduce{};
    std::shared_ptr<grpc::Channel> _channel;
    std::unique_ptr<Rpc::Stub> _stub;
};

#endif
