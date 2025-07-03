#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

struct {
    __uint(type, BPF_MAP_TYPE_SOCKHASH);
    __uint(max_entries, 1024);
    __type(key, __u32);
    __type(value, __u64);
} sock_hash SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 3);
    __type(key, __u32);
    __type(value, __u32);
} port_map SEC(".maps");

SEC("sk_skb/stream_verdict")
int bpf_sockmap_verdict(struct __sk_buff *skb)
{
    __u32 key = 0;
    __u32 *port_info = bpf_map_lookup_elem(&port_map, &key);
    
    if (!port_info) {
        return SK_PASS;
    }
    
    // Extract IP header
    void *data = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;
    
    struct iphdr *iph = data;
    if (data + sizeof(struct iphdr) > data_end) {
        return SK_PASS;
    }
    
    if (iph->protocol != IPPROTO_TCP) {
        return SK_PASS;
    }
    
    struct tcphdr *tcph = data + sizeof(struct iphdr);
    if (data + sizeof(struct iphdr) + sizeof(struct tcphdr) > data_end) {
        return SK_PASS;
    }
    
    __u16 src_port = bpf_ntohs(tcph->source);
    __u16 dst_port = bpf_ntohs(tcph->dest);
    
    // Client (8080) -> Proxy (8081) -> Server (8082)
    if (src_port == 8080 && dst_port == 8081) {
        // Redirect client traffic to server
        __u32 redirect_key = 8082;
        return bpf_sk_redirect_hash(skb, &sock_hash, &redirect_key, BPF_F_INGRESS);
    }
    
    // Server (8082) -> Proxy (8081) -> Client (8080)
    if (src_port == 8082 && dst_port == 8081) {
        // Redirect server response back to client
        __u32 redirect_key = 8080;
        return bpf_sk_redirect_hash(skb, &sock_hash, &redirect_key, BPF_F_INGRESS);
    }
    
    return SK_PASS;
}

SEC("sk_skb/stream_parser")
int bpf_sockmap_parser(struct __sk_buff *skb)
{
    return skb->len;
}

SEC("sockops")
int bpf_sockmap_ops(struct bpf_sock_ops *skops)
{
    __u32 family, op;
    
    family = skops->family;
    op = skops->op;
    
    switch (op) {
    case BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB:
    case BPF_SOCK_OPS_ACTIVE_ESTABLISHED_CB:
        if (family == AF_INET) {
            __u32 key = 0;
            __u16 local_port = bpf_ntohl(skops->local_port) >> 16;
            
            // Map container ports to sockhash keys
            if (local_port == 8080) {
                key = 8080; // Client container
            } else if (local_port == 8081) {
                key = 8081; // Proxy container
            } else if (local_port == 8082) {
                key = 8082; // Server container
            } else {
                return SK_PASS;
            }
            
            // Add socket to sockhash map
            bpf_sock_hash_update(skops, &sock_hash, &key, BPF_NOEXIST);
        }
        break;
    default:
        break;
    }
    
    return SK_PASS;
}

char _license[] SEC("license") = "GPL";