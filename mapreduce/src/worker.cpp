#include "worker.h"
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <cstdlib>
#include <chrono>
#include <thread>
#include <cassert>

#include <grpcpp/grpcpp.h>

using namespace rapidjson;
template <class T>
void KeyValue::json_dump(rapidjson::Writer<T>& writer) const {
    Document d;
    d.SetObject();
    Document::AllocatorType& allocator = d.GetAllocator();

    d.AddMember("Key", StringRef(Key.c_str()), allocator);
    d.AddMember("Value", StringRef(Value.c_str()), allocator);
    d.Accept(writer);
}

bool operator<(const KeyValue& lhs, const KeyValue& rhs) {
    return lhs.Key < rhs.Key;
}

std::ofstream& operator<<(std::ofstream& os, const KeyValue& kv) {
    rapidjson::OStreamWrapper osw(os);
    rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
    kv.json_dump(writer);
    return os;
}

void dump_to_file(const std::string& path, const std::vector<KeyValue>& buckets) {
    std::ofstream out(path);
    for (const auto& kv : buckets) {
        out << kv << "\n";
    }
}

std::vector<KeyValue> load_from_file(const std::string& filename) {
    std::vector<KeyValue> ret;
    std::ifstream ifs(filename);
    std::string json_str;
    int i = 0;
    while (getline(ifs, json_str)) {
        rapidjson::Document d;
        d.Parse(json_str.c_str());
        ret.emplace_back(d["Key"].GetString(), d["Value"].GetString());
    }
    return ret;
}

extern std::vector<KeyValue> Map(const std::string& filename, const std::string& content);
extern std::string Reduce(const std::vector<std::string>& values);

std::pair<MapFunc, ReduceFunc> LoadPlugin() {
    return {Map, Reduce};
}

Worker::Worker()
    : _workerId(-1)
    , _nreduce(0)
    , _channel()
    , _stub()
{
    _channel = grpc::CreateChannel("127.0.0.1:6666", grpc::InsecureChannelCredentials());
    _stub = Rpc::NewStub(_channel);
}

Worker* Worker::make_worker() {
    return new Worker;
}

void Worker::work() {
    auto pair = LoadPlugin();
    while (!done()) {
        // 向 coordinate 索要任务
        AskTaskReply reply = ask_task();
        if (_workerId == -1)
            _workerId = reply.workerid();
        if (has_task(reply)) {
            if (is_map_task(reply)) {
                do_map_task(reply, pair.first);
            } else {
                do_reduce_task(reply, pair.second);
            }
        } else {
            // 暂时没有任务给我做
            // task 都被分配了，但是没有全部完成
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        _times++;
    }
}

bool Worker::done() {
    // 通过 RPC 询问 Coordinator 是否已经完成了所有任务
    IsDoneArgs args;
    IsDoneReply reply;
    ClientContext context;

    Status status = _stub->IsDone(&context, args, &reply);

    if (status.ok()) {
        return reply.done();
    } else {
        // 如果 rpc 超时返回 error 则退出进程
        // 因为 rpc 超时无非两个原因
        // 1. coordinator 完成了所有任务并且已经退出了
        // 2. worker 本地的网络出问题了
        std::exit(1);
    }
}

AskTaskReply Worker::ask_task() {
    AskTaskArgs args;
    args.set_workerid(_workerId);
    AskTaskReply reply;
    ClientContext context;

    Status status = _stub->AskTask(&context, args, &reply);

    if (status.ok()) {
        return reply;
    } else {
        // 如果 rpc 超时返回 error 则退出进程
        // 因为 rpc 超时无非两个原因
        // 1. coordinator 完成了所有任务并且已经退出了
        // 2. worker 本地的网络出问题了
        std::exit(1);
    }
}

ReportTaskReply Worker::report_task(const std::set<std::string>& intermediate_files, int reduce_number) {
    ReportTaskArgs args;
    args.set_workerid(_workerId);
    args.set_xreduce(reduce_number);
    for (const std::string& file : intermediate_files) {
        args.add_intermediatefiles(file);
    }
    ReportTaskReply reply;
    ClientContext context;

    Status status = _stub->ReportTask(&context, args, &reply);

    if (status.ok()) {
        return reply;
    } else {
        // 如果 rpc 超时返回 error 则退出进程
        // 因为 rpc 超时无非两个原因
        // 1. coordinator 完成了所有任务并且已经退出了
        // 2. worker 本地的网络出问题了
        std::exit(1);
    }
}

bool Worker::is_ok() {
    AmIOKArgs args;
    args.set_workerid(_workerId);
    AmIOKReply reply;
    ClientContext context;

    Status status = _stub->AmIOK(&context, args, &reply);

    if (status.ok()) {
        return reply.ok();
    } else {
        // 如果 rpc 超时返回 error 则退出进程
        // 因为 rpc 超时无非两个原因
        // 1. coordinator 完成了所有任务并且已经退出了
        // 2. worker 本地的网络出问题了
        std::exit(1);
    }
}

bool Worker::has_task(const AskTaskReply& reply) { return reply.hastask(); }
bool Worker::is_map_task(const AskTaskReply& reply) { return reply.ismaptask(); }

void Worker::do_map_task(const AskTaskReply& reply, MapFunc mapf) {
    std::string filename = reply.filename();
    assert(filename != "");
    std::string input_key = filename;

    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd == -1) {
        ::fprintf(stderr, "Can not open file %s. Error: %s.\n", filename.c_str(), 
                ::strerror(errno));
        ::exit(1);
    }
    std::string input_value;
    char buf[4096] = {{0}};
    int n = 0;
    while ((n = ::read(fd, buf, sizeof buf)) > 0) {
        input_value += std::string(buf, n);
    }
    if (n < 0) {
        ::fprintf(stderr, "Read file %s error. Error: %s.\n", filename.c_str(), 
                ::strerror(errno));
        ::exit(1);
    }
    ::close(fd);

    std::vector<KeyValue> kvs = mapf(input_key, input_value);

    int nreduce = reply.nreduce();
    std::vector<std::vector<KeyValue>> buckets(nreduce, std::vector<KeyValue>());
    std::hash<std::string> hash_func;
    for (KeyValue& kv : kvs) {
        size_t hash_result = hash_func(kv.Key);
        buckets[hash_result % nreduce].push_back(kv);
    }
    std::set<std::string> intermediate_files;
    // mapBaseName-workerId-times-xreduce
    for (int i = 0; i < nreduce; ++i) {
        std::string suffix = "-" + std::to_string(_workerId) + "-" + std::to_string(_times) + 
            "-" + std::to_string(i);
        std::string path = reply.mapbasename() + suffix;
        dump_to_file(path, buckets[i]);
        intermediate_files.insert(path);
    }
    report_task(intermediate_files, -1);
}

void Worker::do_reduce_task(const AskTaskReply& reply, ReduceFunc reducef) {
    // 从中间文件中挑选出以 Xreduce 结尾的文件
    int xreduce = reply.xreduce();

    std::vector<std::string> files;
    for (int i = 0; i < reply.intermediatefiles_size(); ++i) {
        std::string filename = reply.intermediatefiles(i);
        std::string::size_type pos = filename.rfind("-");
        if (pos == std::string::npos) {
            fprintf(stderr, "Can not find - in file name %s.\n", filename.c_str());
            ::exit(1);
        }
        if (std::atoi(filename.substr(pos + 1).c_str()) == xreduce)
            files.push_back(filename); 
    }

    // 取出所有的 kvs
    std::vector<KeyValue> x_kvs; // 存储所有以 Xreduce 结尾的文件中的 kvs
    for (std::string& file : files) {
        auto kvs = load_from_file(file); 
        x_kvs.insert(x_kvs.end(), kvs.begin(), kvs.end());
    }
    sort(x_kvs.begin(), x_kvs.end());

    std::vector<KeyValue> final_results;
    // 执行 reduce 函数
    int i = 0;
    while (i < x_kvs.size()) {
        std::vector<std::string> values;
        values.push_back(x_kvs[i].Value);
        int j = i + 1;
        while (j < x_kvs.size() and x_kvs[j].Key == x_kvs[i].Key) {
            values.push_back(x_kvs[j].Value);
            ++j;
        }
        final_results.push_back({x_kvs[i].Key, reducef(values)});
        i = j;
        ///////////////////////////////////////////////////////////////////
    }

    if (is_ok()) {
        std::string output_f = reply.reducebasename() + "-" + std::to_string(xreduce);
        FILE* fd = ::fopen(output_f.c_str(), "w");
        if (fd == NULL) {
            ::fprintf(stderr, "Can not open file %s. Error: %s.\n", output_f.c_str(), 
                    ::strerror(errno));
            ::exit(1);
        }
        for (auto& kv : final_results) {
            fprintf(fd, "%s %s\n", kv.Key.c_str(), kv.Value.c_str());
        }
        ::fclose(fd);
        report_task({}, xreduce);
    }
}
