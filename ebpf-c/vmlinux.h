/* SPDX-License-Identifier: GPL-2.0 */
#ifndef __VMLINUX_H__
#define __VMLINUX_H__

typedef unsigned char __u8;
typedef short __s16;
typedef unsigned short __u16;
typedef int __s32;
typedef unsigned int __u32;
typedef long long __s64;
typedef unsigned long long __u64;
typedef __u16 __be16;
typedef __u32 __be32;
typedef __u32 __wsum;

#define AF_INET 2
#define IPPROTO_TCP 6

// BPF constants
#define BPF_MAP_TYPE_SOCKHASH 18
#define BPF_MAP_TYPE_ARRAY 2
#define SK_PASS 1
#define BPF_F_INGRESS (1U << 0)
#define BPF_NOEXIST 1
#define BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB 1
#define BPF_SOCK_OPS_ACTIVE_ESTABLISHED_CB 2

struct iphdr {
    __u8 ihl:4,
         version:4;
    __u8 tos;
    __u16 tot_len;
    __u16 id;
    __u16 frag_off;
    __u8 ttl;
    __u8 protocol;
    __u16 check;
    __u32 saddr;
    __u32 daddr;
};

struct tcphdr {
    __u16 source;
    __u16 dest;
    __u32 seq;
    __u32 ack_seq;
    __u16 res1:4,
          doff:4,
          fin:1,
          syn:1,
          rst:1,
          psh:1,
          ack:1,
          urg:1,
          ece:1,
          cwr:1;
    __u16 window;
    __u16 check;
    __u16 urg_ptr;
};

struct __sk_buff {
    __u32 len;
    __u32 pkt_type;
    __u32 mark;
    __u32 queue_mapping;
    __u32 protocol;
    __u32 vlan_present;
    __u32 vlan_tci;
    __u32 vlan_proto;
    __u32 priority;
    __u32 ingress_ifindex;
    __u32 ifindex;
    __u32 tc_index;
    __u32 cb[5];
    __u32 hash;
    __u32 tc_classid;
    __u32 data;
    __u32 data_end;
    __u32 napi_id;
    __u32 family;
    __u32 remote_ip4;
    __u32 local_ip4;
    __u32 remote_ip6[4];
    __u32 local_ip6[4];
    __u32 remote_port;
    __u32 local_port;
    __u32 data_meta;
    __u32 flow_keys;
    __u64 tstamp;
    __u32 wire_len;
    __u32 gso_segs;
    __u32 sk;
    __u32 gso_size;
    __u32 tstamp_type;
    __u64 hwtstamp;
};

struct bpf_sock_ops {
    __u32 op;
    union {
        __u32 args[4];
        __u32 reply;
        __u32 replylong[4];
    };
    __u32 family;
    __u32 remote_ip4;
    __u32 local_ip4;
    __u32 remote_ip6[4];
    __u32 local_ip6[4];
    __u32 remote_port;
    __u32 local_port;
    __u32 is_fullsock;
    __u32 snd_cwnd;
    __u32 srtt_us;
    __u32 bpf_sock_ops_cb_flags;
    __u32 state;
    __u32 rtt_min;
    __u32 snd_ssthresh;
    __u32 rcv_nxt;
    __u32 snd_nxt;
    __u32 snd_una;
    __u32 mss_cache;
    __u32 ecn_flags;
    __u32 rate_delivered;
    __u32 rate_interval_us;
    __u32 packets_out;
    __u32 retrans_out;
    __u32 total_retrans;
    __u32 segs_in;
    __u32 data_segs_in;
    __u32 segs_out;
    __u32 data_segs_out;
    __u32 lost_out;
    __u32 sacked_out;
    __u32 sk_txhash;
    __u64 bytes_received;
    __u64 bytes_acked;
    __u32 sk;
    __u32 skb_data;
    __u32 skb_data_end;
    __u32 skb_len;
    __u32 skb_tcp_flags;
    __u64 skb_hwtstamp;
};

#endif