mpirun --allow-run-as-root -np 4 -H node1:1,node2:1,node3:1,node4:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by slot uv run test.py
