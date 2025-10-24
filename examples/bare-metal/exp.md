## Three arbutus machines

1. Running FDSP as controller and database.
2. Running ns-test as node1. 
3. Running ns-test2 as node2.



test1:

```
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 5, 7.561419ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 6, 7.41831ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 6, 9.000746ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 7, 7.299328ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 7, 4.999779ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 8, 6.306473ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 8, 6.782209ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 9, 5.164575ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] avg over 10 reps: 9.685315ms
[rank=0@ubuntu@206.12.95.232] ssh exited with code 0
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 9, 5.916994ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] avg over 10 reps: 9.310683ms
```

test2:

rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 2, 8.926747ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 2, 8.829053ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 3, 7.098286ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 3, 9.230065ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 4, 8.277347ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 4, 8.570187ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 5, 5.721909ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 5, 5.184803ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 6, 5.897901ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 6, 5.49338ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 7, 5.743771ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 7, 6.197668ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 8, 5.446525ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 8, 8.779343ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 9, 7.002754ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] avg over 10 reps: 9.101603ms
[rank=1@ubuntu@206.12.91.229] ssh exited with code 0
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 9, 7.828316ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] avg over 10 reps: 9.907935ms



Raw test:

[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 5, 3.323488ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 6, 3.575293ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 6, 4.027179ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 7, 3.222313ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 7, 3.40414ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 8, 3.224918ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 8, 3.647901ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] verification OK (rep 9, 3.167303ms).
[rank=1@ubuntu@206.12.91.229] [rank 1] avg over 10 reps: 4.190457ms
[rank=0@ubuntu@206.12.95.232] [rank 0] verification OK (rep 9, 3.213379ms).
[rank=0@ubuntu@206.12.95.232] [rank 0] avg over 10 reps: 3.983737ms