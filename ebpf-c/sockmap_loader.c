#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>

struct bpf_program_info {
    struct bpf_object *obj;
    int sock_hash_fd;
    int port_map_fd;
    int verdict_prog_fd;
    int parser_prog_fd;
    int sockops_prog_fd;
};

static int load_bpf_program(struct bpf_program_info *info)
{
    struct bpf_program *prog;
    struct bpf_map *map;
    int err;

    // Load BPF object
    info->obj = bpf_object__open_file("sockmap_redirect.o", NULL);
    if (libbpf_get_error(info->obj)) {
        fprintf(stderr, "Failed to open BPF object file\n");
        return -1;
    }

    // Load BPF program
    err = bpf_object__load(info->obj);
    if (err) {
        fprintf(stderr, "Failed to load BPF object: %s\n", strerror(-err));
        return -1;
    }

    // Get maps
    map = bpf_object__find_map_by_name(info->obj, "sock_hash");
    if (!map) {
        fprintf(stderr, "Failed to find sock_hash map\n");
        return -1;
    }
    info->sock_hash_fd = bpf_map__fd(map);

    map = bpf_object__find_map_by_name(info->obj, "port_map");
    if (!map) {
        fprintf(stderr, "Failed to find port_map\n");
        return -1;
    }
    info->port_map_fd = bpf_map__fd(map);

    // Get programs
    prog = bpf_object__find_program_by_name(info->obj, "bpf_sockmap_verdict");
    if (!prog) {
        fprintf(stderr, "Failed to find verdict program\n");
        return -1;
    }
    info->verdict_prog_fd = bpf_program__fd(prog);

    prog = bpf_object__find_program_by_name(info->obj, "bpf_sockmap_parser");
    if (!prog) {
        fprintf(stderr, "Failed to find parser program\n");
        return -1;
    }
    info->parser_prog_fd = bpf_program__fd(prog);

    prog = bpf_object__find_program_by_name(info->obj, "bpf_sockmap_ops");
    if (!prog) {
        fprintf(stderr, "Failed to find sockops program\n");
        return -1;
    }
    info->sockops_prog_fd = bpf_program__fd(prog);

    return 0;
}

static int attach_sockops_program(int prog_fd, const char *cgroup_path)
{
    int cgroup_fd;
    int err;

    cgroup_fd = open(cgroup_path, O_RDONLY);
    if (cgroup_fd < 0) {
        fprintf(stderr, "Failed to open cgroup: %s\n", strerror(errno));
        return -1;
    }

    err = bpf_prog_attach(prog_fd, cgroup_fd, BPF_CGROUP_SOCK_OPS, 0);
    if (err) {
        fprintf(stderr, "Failed to attach sockops program: %s\n", strerror(-err));
        close(cgroup_fd);
        return -1;
    }

    close(cgroup_fd);
    return 0;
}

static int attach_sk_skb_programs(struct bpf_program_info *info)
{
    int err;

    // Attach verdict program to sockhash
    err = bpf_prog_attach(info->verdict_prog_fd, info->sock_hash_fd, 
                          BPF_SK_SKB_STREAM_VERDICT, 0);
    if (err) {
        fprintf(stderr, "Failed to attach verdict program: %s\n", strerror(-err));
        return -1;
    }

    // Attach parser program to sockhash
    err = bpf_prog_attach(info->parser_prog_fd, info->sock_hash_fd, 
                          BPF_SK_SKB_STREAM_PARSER, 0);
    if (err) {
        fprintf(stderr, "Failed to attach parser program: %s\n", strerror(-err));
        return -1;
    }

    return 0;
}

static void cleanup(struct bpf_program_info *info)
{
    if (info->obj) {
        bpf_object__close(info->obj);
    }
}

int main(int argc, char **argv)
{
    struct bpf_program_info info = {0};
    int err;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <cgroup_path>\n", argv[0]);
        return 1;
    }

    // Load BPF program
    err = load_bpf_program(&info);
    if (err) {
        fprintf(stderr, "Failed to load BPF program\n");
        return 1;
    }

    // Attach sockops program to cgroup
    err = attach_sockops_program(info.sockops_prog_fd, argv[1]);
    if (err) {
        fprintf(stderr, "Failed to attach sockops program\n");
        cleanup(&info);
        return 1;
    }

    // Attach SK_SKB programs to sockhash
    err = attach_sk_skb_programs(&info);
    if (err) {
        fprintf(stderr, "Failed to attach SK_SKB programs\n");
        cleanup(&info);
        return 1;
    }

    printf("BPF programs loaded and attached successfully\n");
    printf("Press Ctrl+C to exit\n");

    // Keep the program running
    while (1) {
        sleep(1);
    }

    cleanup(&info);
    return 0;
}