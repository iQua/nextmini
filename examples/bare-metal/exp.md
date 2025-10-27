## Four machines

Configuration:

```text
root@157.180.84.40
root@174.138.23.4
root@134.209.191.238
root@134.199.159.159
```

---

## Nextmini 4MB

```text
[rank=2@root@134.209.191.238] [rank 2] verification OK (rep 9, 871.447658ms).
[rank=2@root@134.209.191.238] [rank 2] avg over 10 reps: 1.09328626s
[rank=2@root@134.209.191.238] ssh exited with code 0
[rank=3@root@134.199.159.159] [rank 3] verification OK (rep 9, 830.300587ms).
[rank=3@root@134.199.159.159] [rank 3] avg over 10 reps: 1.143029512s
[rank=3@root@134.199.159.159] ssh exited with code 0
[rank=0@root@157.180.84.40] [rank 0] verification OK (rep 9, 1.04363117s).
[rank=0@root@157.180.84.40] [rank 0] avg over 10 reps: 1.13882251s
[rank=0@root@157.180.84.40] ssh exited with code 0
[rank=1@root@174.138.23.4] [rank 1] verification OK (rep 9, 1.078597111s).
[rank=1@root@174.138.23.4] [rank 1] avg over 10 reps: 1.125984307s
[rank=1@root@174.138.23.4] ssh exited with code 0
```

## Nextmini 500MB

```text
[rank=1@root@174.138.23.4] [rank 1] verification OK (rep 0, 101.883918263s).
[rank=0@root@157.180.84.40] [rank 0] verification OK (rep 0, 106.032296123s).
[rank=2@root@134.209.191.238] [rank 2] verification OK (rep 0, 103.269682919s).
[rank=3@root@134.199.159.159] [rank 3] verification OK (rep 0, 104.389912891s).
```

## Physical 4MB:

```text
[rank=2@root@134.209.191.238] [rank 2] verification OK (rep 9, 741.071051ms).
[rank=2@root@134.209.191.238] [rank 2] avg over 10 reps: 803.832818ms
[rank=2@root@134.209.191.238] ssh exited with code 0
[rank=0@root@157.180.84.40] [rank 0] verification OK (rep 9, 674.369721ms).
[rank=0@root@157.180.84.40] [rank 0] avg over 10 reps: 784.177123ms
[rank=0@root@157.180.84.40] ssh exited with code 0
[rank=1@root@174.138.23.4] [rank 1] verification OK (rep 9, 663.900077ms).
[rank=1@root@174.138.23.4] [rank 1] avg over 10 reps: 816.057077ms
[rank=1@root@174.138.23.4] ssh exited with code 0
[rank=3@root@134.199.159.159] [rank 3] verification OK (rep 9, 751.026567ms).
[rank=3@root@134.199.159.159] [rank 3] avg over 10 reps: 804.52533ms
```

## Physical 500MB:

```text
[rank=2@root@134.209.191.238] [rank 2] avg over 2 reps: 68.870307025s
[rank=2@root@134.209.191.238] ssh exited with code 0
[rank=0@root@157.180.84.40] [rank 0] verification OK (rep 1, 69.388050571s).
[rank=0@root@157.180.84.40] [rank 0] avg over 2 reps: 71.823301742s
[rank=0@root@157.180.84.40] ssh exited with code 0
[rank=1@root@174.138.23.4] [rank 1] verification OK (rep 1, 67.48237481s).
[rank=1@root@174.138.23.4] [rank 1] avg over 2 reps: 69.696862648s
[rank=1@root@174.138.23.4] ssh exited with code 0
[rank=3@root@134.199.159.159] [rank 3] verification OK (rep 1, 66.988386249s).
[rank=3@root@134.199.159.159] [rank 3] avg over 2 reps: 69.287617738s
```