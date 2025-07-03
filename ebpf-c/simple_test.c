#include "vmlinux.h"
#include <bpf/bpf_helpers.h>

// Simple array map for testing
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} test_map SEC(".maps");

SEC("socket")
int test_prog(struct __sk_buff *skb)
{
    __u32 key = 0;
    __u64 *count = bpf_map_lookup_elem(&test_map, &key);
    if (count) {
        (*count)++;
    }
    return SK_PASS;
}

char _license[] SEC("license") = "GPL";