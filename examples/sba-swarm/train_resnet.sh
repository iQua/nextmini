mpirun --allow-run-as-root \
  -np 2 \
  -H 10.0.0.1:1,10.0.0.2:1 \
  -x MASTER_ADDR=node1 \
  -x MASTER_PORT=1234 \
  -x PATH \
  -bind-to none \
  -map-by slot \
  sh -c 'export RANK=$OMPI_COMM_WORLD_RANK; export WORLD_SIZE=$OMPI_COMM_WORLD_SIZE; uv run resnet.py --type resnet34'
