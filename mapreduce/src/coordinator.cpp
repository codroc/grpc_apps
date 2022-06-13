#include "coordinator.h"
#include <stdlib.h>
#include <stdio.h>
#include <thread>

Coordinator* Coordinator::make_coordinator(const std::vector<std::string>& files, uint32_t nreduce) {
    auto ret = new Coordinator();
    ret->_remainMapTasks = files.size();
    ret->_remainReduceTasks = nreduce;
    ret->_nreduce = nreduce;
    ret->_files = files;
    for (const auto& file : files) {
        ret->_mapTasks.insert({file, 0});
    }
    for (int i = 0; i < nreduce; ++i) {
        ret->_reduceTasks.insert({i, 0});
    }
    return ret;
}

// 不是线程安全的，调用者需要加锁
bool Coordinator::done() {
    bool ret = _mapTaskIsDone and _reduceTaskIsDone; 
    for (auto& [key, value] : _timers) {
        assert(_ws.find(key) != _ws.end());
        if (value == 8 and _ws[key] == Busy) {
            _ws[key] = Timeout;
            if (!_mapTaskIsDone) {
                _mapTasks[_workerToMapTask[key]] = 0;
                _workerToMapTask.erase(key);
            } else if (!_reduceTaskIsDone) {
                _reduceTasks[_workerToReduceTask[key]] = 0;
                _workerToReduceTask.erase(key);
            }
        } else {
            _timers[key]++;
        }
    }
    return ret;
}

Status Coordinator::IsDone(ServerContext* context, const IsDoneArgs* args, IsDoneReply* reply) {
    std::lock_guard<std::mutex> guard(_mu);

    reply->set_done(done());
    return Status::OK;
}

Status Coordinator::AskTask(ServerContext* context, const AskTaskArgs* args, AskTaskReply* reply) {
    std::lock_guard<std::mutex> guard(_mu);

    // 如果是一个新的 worker 则给他分配 worker id
    uint32_t worker_id = args->workerid();
    if (args->workerid() == -1) {
        worker_id = _workers.fetch_add(1, std::memory_order_relaxed);
        _ws[worker_id] = Free;
    }
    reply->set_workerid(worker_id);

    if (!_mapTaskIsDone) {
        reply->set_ismaptask(true);
        reply->set_nreduce(_nreduce);
        reply->set_mapbasename("mr");

        std::string task;
        for (auto& [key,value] : _mapTasks) {
            if (value == 0) {
                task = key;
                _mapTasks[key] = 1;
                break;
            }
        }
        if (task != "") {
            reply->set_hastask(true);
            reply->set_filename(task);

            _workerToMapTask[worker_id] = task;
            _ws[worker_id] = Busy;
            _timers[worker_id] = 0;
        }
    } else if (!_reduceTaskIsDone) {
        reply->set_isreducetask(true);
        reply->set_reducebasename("mr-out");
        
        int xreduce = -1;
        for (auto& [key, value] : _reduceTasks) {
            if (value == 0) {
                xreduce = key;
                _reduceTasks[key] = 1;
                break;
            }
        }
        if (xreduce != -1) {
            reply->set_hastask(true);
            reply->set_xreduce(xreduce);
            for (const auto& file : _intermediateFiles) {
                reply->add_intermediatefiles(file);
            }

            _workerToReduceTask[worker_id] = xreduce;
            _ws[worker_id] = Busy;
            _timers[worker_id] = 0;
        }
    } else {
        reply->set_hastask(false);
    }

    return Status::OK;
}

Status Coordinator::ReportTask(ServerContext* context, const ReportTaskArgs* args, ReportTaskReply* reply) {
    std::lock_guard<std::mutex> guard(_mu);

    // 这里来报告的 worker 都是我 coordinator 认可的工人，它们做的任务都是有效的
    int worker_id = args->workerid();
    if (!_mapTaskIsDone) {
        int n = args->intermediatefiles_size();
        assert(n > 0);
        for (int i = 0; i < n; ++i) {
            _intermediateFiles.push_back(args->intermediatefiles(i));
        }

        _mapTasks[args->filename()] = 2;
        _workerToMapTask.erase(worker_id);
        _ws[worker_id] = Free;

        _remainMapTasks--;
        if (_remainMapTasks == 0)
            _mapTaskIsDone = true;
    } else if (!_reduceTaskIsDone) {
        assert(args->xreduce() != -1);

        _reduceTasks[args->xreduce()] = 2;
        _workerToReduceTask.erase(worker_id);
        _ws[worker_id] = Free;

        _remainReduceTasks--;
        if (_remainReduceTasks == 0)
            _reduceTaskIsDone = true;
    } else {
    }

    return Status::OK;
}

Status Coordinator::AmIOK(ServerContext* context, const AmIOKArgs* args, AmIOKReply* reply) {
    std::lock_guard<std::mutex> guard(_mu);

    int worker_id = args->workerid();
    assert(_ws.find(worker_id) != _ws.end());
    if (_ws[worker_id] == Timeout) {
        reply->set_ok(false);
    } else {
        reply->set_ok(true);
    }
    
    return Status::OK;
}

void Coordinator::run() {
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    std::string server_address("127.0.0.1:6666");
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    printf("Coordinator starts to work!\n");

    while (!done()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    server->Shutdown();
}
