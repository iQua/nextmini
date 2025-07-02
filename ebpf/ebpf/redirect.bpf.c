#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

// Socket map to store socket pairs
struct {
    __uint(type, BPF_MAP_TYPE_SOCKMAP);
    __uint(max_entries, 1024);
    __type(key, __u32);
    __type(value, __u64);
} sock_map SEC(".maps");

SEC("sk_msg")
int tcp_redirect(struct sk_msg_md *msg)
{
    __u32 key = msg->sk->src_port;
    __u64 *redirect_fd;
    
    // Look up partner socket
    redirect_fd = bpf_map_lookup_elem(&sock_map, &key);
    if (!redirect_fd) {
        return SK_PASS;  // Let userspace handle
    }
    
    // Redirect to partner socket
    return bpf_msg_redirect_map(msg, &sock_map, *redirect_fd, BPF_F_INGRESS);
}

char _license[] SEC("license") = "GPL";