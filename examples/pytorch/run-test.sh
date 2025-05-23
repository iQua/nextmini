mpirun --allow-run-as-root -np 4 -H 10.0.0.1:1,10.0.0.2:1,10.0.0.3:1,10.0.0.4:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by slot uv run test.py
