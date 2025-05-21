horovodrun -np 12 -H horovod1:2,horovod2:2,horovod3:2,horovod4:2,horovod5:2,horovod6:2 --mpi-args="--mca btl_base_verbose 100" --network-interfaces utun0 python $@
