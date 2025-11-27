# Adept: Asynchronous Deterministic Transaction Processing for Distributed Databases


This repository hosts the reference implementation of **Adept**, an asynchronous deterministic transaction processing protocol for distributed databases. This system is designed to provide high throughput while maintaining deterministic guarantees.

This codebase is built upon and extends [Aria](https://github.com/luyi0619/aria) (VLDB 2020).

## System Requirements

- **OS**: Linux (Tested on Ubuntu/Debian)
- **Compiler**: g++ (Supports C++20)
- **Build System**: CMake / Make

## Dependencies

To set up the development environment on Ubuntu/Debian, install the following packages:

```bash
sudo apt-get update
sudo apt-get install -y zip make cmake g++ libjemalloc-dev libboost-dev libgoogle-glog-dev
```

## Compilation
To compile the database system and benchmarks:
```sh
chmod +x compile.sh
./compile.sh
```
Upon successful compilation, the benchmark binaries (e.g., bench_ycsb, bench_tpcc) will be generated in the project root or binary directory.

## Usage & Benchmarking

The system supports standard OLTP benchmarks: YCSB and TPC-C.

### Command Line Arguments

Commonly used configuration flags are described below:
| Flag | Description |
| :--- | :--- |
| `--protocol` | The transaction protocol to use (e.g., `Adept`, `Aria`). |
| `--servers` | Semicolon-separated list of server IP addresses (e.g., `10.0.0.1:port;10.0.0.2:port`). |
| `--id` | The ID of the current server node (0-indexed). |
| `--partition_num` | Total number of partitions across the cluster. |
| `--threads` | Number of worker threads per node. |
| `--read_write_ratio` | Percentage of read/write transactions (e.g., `80` for 80% read). |
| `--cross_ratio` | Percentage of cross-partition transactions. |
| `--zipf` | Zipfian skew factor for data access distribution. |

### Running a YCSB Example

```sh
./bench_ycsb --log_dir=./log/test 
  --protocol=Adept 
  --servers="10.0.0.1:10000;10.0.0.2:10001" 
  --id=0 
  --partition_num=12 
  --threads=8 
  --batch_size=1000 
  --read_write_ratio=80 
  --cross_ratio=15 
  --keys=40000 
  --zipf=0.999 
  --lock_manager=1 
  --replica_group=2 
  --same_batch=False 
  --two_partitions=True 
  --cpu_affinity=false 
  --delay=500
  --mvcc=true 
  --blocked_optimize=true
```

Note: Ensure that the servers list is consistent across all nodes and id corresponds to the machine executing the command.