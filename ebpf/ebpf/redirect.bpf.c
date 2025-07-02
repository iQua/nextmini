#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

// Sockhash map, mapping raw fds
struct {
    __uint(type, BPF_MAP_TYPE_SOCKHASH);
    __uint(max_entries, 1024);
    __type(key, __u32); 
} sock_hash SEC(".maps");

SEC("sk_msg")
int tcp_redirect(struct sk_msg_md *msg)
{
    __u32 current_fd = (__u32)(unsigned long)msg->sk;
    
    return bpf_msg_redirect_hash(msg, &sock_hash, &current_fd, BPF_F_INGRESS);
}

char _license[] SEC("license") = "GPL";