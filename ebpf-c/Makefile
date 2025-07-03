CC = clang
CFLAGS = -O2 -g -Wall -target bpf -I/usr/include/$(shell uname -m)-linux-gnu
LDFLAGS = -lbpf

# BPF object files
BPF_OBJ = sockmap_redirect.o simple_test.o

# User space programs
USER_PROGS = sockmap_loader test_loader

.PHONY: all clean

all: $(BPF_OBJ) $(USER_PROGS)

$(BPF_OBJ): sockmap_redirect.c
	$(CC) $(CFLAGS) -c $< -o $@

sockmap_loader: sockmap_loader.c
	gcc -o $@ $< $(LDFLAGS)

test_loader: test_loader.c
	gcc -o $@ $< $(LDFLAGS)

clean:
	rm -f $(BPF_OBJ) $(USER_PROGS)

install: all
	sudo ./sockmap_loader /sys/fs/cgroup/unified