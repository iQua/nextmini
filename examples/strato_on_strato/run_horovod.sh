horovodrun -np 3 -H localhost:1,horovod2:1,horovod3:1 --mpi-args="--mca btl_base_verbose 100" --network-interfaces utun --timeline-filename /mnt/shared/strato/timeline.json python $@
