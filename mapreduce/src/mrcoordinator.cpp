#include "coordinator.h"
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char** argv) {
    if (argc < 2) {
        ::fprintf(stderr, "usage: %s files...\n", argv[0]);
        ::exit(1);
    }

    std::vector<std::string> files;
    for (int i = 1; i < argc; ++i) {
        files.emplace_back(argv[i]);
    }
    uint32_t nreduce = 10;

    std::shared_ptr<Coordinator> coordinator(Coordinator::make_coordinator(files, nreduce));
    coordinator->run();

    printf("All work done!\n");
    return 0;
}
