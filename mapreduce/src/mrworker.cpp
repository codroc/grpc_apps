#include "worker.h"
#include <stdio.h>

int main() {
    std::shared_ptr<Worker> worker(Worker::make_worker());
    printf("Worker starts work!\n");
    worker->work();
    printf("Worker finished work!\n");
    return 0;
}
