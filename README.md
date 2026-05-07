# final-project-concurrent-task-dispatcher

# Concurrent Task Dispatcher in Rust

## Project Summary

This project simulates a concurrent task dispatcher using Rust threads, queues, and worker pools. The program generates 1000 tasks over time and dispatches them to 8 worker threads while keeping total CPU usage under 100%.

The project compares two scheduling methods:
- FIFO scheduling
- Optimized scheduling

IO tasks use 10% CPU and CPU tasks use 35% CPU. Each task runs for 200 ms.

---

## Architecture

The system uses:
- 1 main thread
- 1 generator thread
- 1 manager/dispatcher thread
- 1 monitor thread
- 8 worker threads

The generator creates tasks and sends them to the manager. The manager places tasks into queues and dispatches them to workers if CPU resources and workers are available.

---

## Data Structures

The project uses:
- `VecDeque<Task>` for queues
- `Arc<Mutex<SystemState>>` for shared state
- `mpsc` channels for communication between threads

---

## Scheduling Policies

### FIFO
Uses one queue and processes tasks in arrival order.

### Optimized
Uses separate CPU and IO queues and tries to better manage CPU usage while keeping workers active.

---

## Metrics Collected

The program records:
- total runtime
- average wait time
- average turnaround time
- completed tasks
- CPU usage
- active workers

The monitor thread logs CPU usage and worker activity every 10 ms into CSV files.

---

## How to Build and Run

Run the project:

```bash
cd concurrent-task-dispatcher-rust

cargo run
