mpirun --allow-run-as-root -np 2 -H 10.0.0.1:1,10.0.0.2:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by :OVERSUBSCRIBE uv run --python 3.12 test.py
