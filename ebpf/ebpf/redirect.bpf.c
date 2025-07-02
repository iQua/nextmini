#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

// Sockhash map
struct {
    __uint(type, BPF_MAP_TYPE_SOCKHASH);
    __uint(max_entries, 1024);
    __type(key, __u32);
} sock_hash SEC(".maps");

// Map by remote port
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 1024);
    __type(key, __u32);    
    __type(value, __u32);  
} port_map SEC(".maps");

SEC("sk_msg")
int tcp_redirect(struct sk_msg_md *msg)
{
    __u32 src_port = msg->remote_port;  
    __u32 *target_port;
    
    target_port = bpf_map_lookup_elem(&port_map, &src_port);
    if (!target_port) {
        return SK_PASS;
    }
    
    return bpf_msg_redirect_hash(msg, &sock_hash, target_port, BPF_F_INGRESS);
}

char _license[] SEC("license") = "GPL";