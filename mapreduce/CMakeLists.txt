cmake_minimum_required(VERSION 3.5.1)
project(mapreduce)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED OFF)

# 生成 grpc pb .cc .h 文件
get_filename_component(rpc_proto "./protos/rpc.proto" ABSOLUTE)
get_filename_component(rpc_proto_path "${rpc_proto}" DIRECTORY)

set(rpc_proto_srcs "${CMAKE_BINARY_DIR}/rpc.pb.cc")
set(rpc_proto_hdrs "${CMAKE_BINARY_DIR}/rpc.pb.h")
set(rpc_grpc_srcs "${CMAKE_BINARY_DIR}/rpc.grpc.pb.cc")
set(rpc_grpc_hdrs "${CMAKE_BINARY_DIR}/rpc.grpc.pb.h")

find_program(PROTOC protoc)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)

add_custom_command(
    OUTPUT "${rpc_proto_srcs}" "${rpc_proto_hdrs}" "${rpc_grpc_srcs}" "${rpc_grpc_hdrs}"
    COMMAND ${PROTOC}
    ARGS --cpp_out "${CMAKE_BINARY_DIR}"
    --grpc_out "${CMAKE_BINARY_DIR}"
    -I "${rpc_proto_path}"
    --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
    "${rpc_proto}"
    DEPENDS "${rpc_proto}")
include_directories("${CMAKE_BINARY_DIR}" "./include")

# 找 grpc 和 protobuf 的 静态/共享 库
find_package(Threads REQUIRED)

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
if(Protobuf_FOUND)
	message("Found!")
else(Protobuf_FOUND)
	message(FATAL_ERROR "Not Found!")
endif(Protobuf_FOUND)

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)


find_package(gRPC CONFIG REQUIRED)
if(gRPC_FOUND)
	message("Found!")
else(gRPC_FOUND)
	message(FATAL_ERROR "Not Found!")
endif(gRPC_FOUND)

set(_GRPC_GRPCPP gRPC::grpc++)

# 添加 rpc.proto 的库 以及 可执行文件
add_library(rpc_grpc_proto
    ${rpc_proto_srcs}
    ${rpc_proto_hdrs}
    ${rpc_grpc_srcs}
    ${rpc_grpc_hdrs})
target_link_libraries(rpc_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

foreach(_target
        mrcoordinator mrworker)
    add_executable(${_target} ./mrapps/wc.cpp ./src/coordinator.cpp ./src/worker.cpp ./src/${_target}.cpp)
    target_link_libraries(${_target}
        rpc_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})
endforeach()
