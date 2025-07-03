#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>

int main()
{
    struct bpf_object *obj;
    struct bpf_program *prog;
    struct bpf_map *map;
    int prog_fd, map_fd;
    int err;

    // Load BPF object
    obj = bpf_object__open_file("simple_test.o", NULL);
    if (libbpf_get_error(obj)) {
        fprintf(stderr, "Failed to open BPF object file\n");
        return -1;
    }

    // Load BPF program
    err = bpf_object__load(obj);
    if (err) {
        fprintf(stderr, "Failed to load BPF object: %s\n", strerror(-err));
        return -1;
    }

    // Get program
    prog = bpf_object__find_program_by_name(obj, "test_prog");
    if (!prog) {
        fprintf(stderr, "Failed to find test program\n");
        return -1;
    }
    prog_fd = bpf_program__fd(prog);

    // Get map
    map = bpf_object__find_map_by_name(obj, "test_map");
    if (!map) {
        fprintf(stderr, "Failed to find test map\n");
        return -1;
    }
    map_fd = bpf_map__fd(map);

    printf("BPF program loaded successfully!\n");
    printf("Program FD: %d\n", prog_fd);
    printf("Map FD: %d\n", map_fd);

    // Test map operations
    __u32 key = 0;
    __u64 value = 42;
    
    err = bpf_map_update_elem(map_fd, &key, &value, BPF_ANY);
    if (err) {
        fprintf(stderr, "Failed to update map: %s\n", strerror(-err));
    } else {
        printf("Map update successful!\n");
    }

    __u64 read_value;
    err = bpf_map_lookup_elem(map_fd, &key, &read_value);
    if (err) {
        fprintf(stderr, "Failed to read map: %s\n", strerror(-err));
    } else {
        printf("Map read successful! Value: %llu\n", read_value);
    }

    bpf_object__close(obj);
    return 0;
}