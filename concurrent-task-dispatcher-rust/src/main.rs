use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const TOTAL_TASKS: usize = 1000;
const WORKER_COUNT: usize = 8;
const ARRIVAL_INTERVAL_MS: u64 = 20;
const TASK_DURATION_MS: u64 = 200;
const CPU_LIMIT: u32 = 100;

#[derive(Clone, Debug)]
enum TaskKind {
    IO,
    CPU,
}

#[derive(Clone, Debug)]
struct Task {
    id: usize,
    arrival_time: u128,
    kind: TaskKind,
    duration_ms: u64,
    cpu_cost: u32,
}

#[derive(Clone)]
struct SystemState {
    current_cpu: u32,
    active_workers: usize,
    done: bool,
}

struct CompletedTask {
    task: Task,
    start_time: u128,
    finish_time: u128,
}

enum ManagerMessage {
    NewTask(Task),
    WorkerDone(CompletedTask),
    GeneratorDone,
}

enum WorkerMessage {
    Run(Task),
    Shutdown,
}

#[derive(Clone)]
enum Policy {
    Fifo,
    Optimized,
}

fn main() {
    let fifo_results = run_simulation("FIFO simple simulation", Policy::Fifo, 70);
    let optimized_results = run_simulation("Optimized simulation", Policy::Optimized, 70);

    println!();
    println!("===== Final Comparison =====");
    println!("FIFO runtime:       {} ms", fifo_results.total_runtime);
    println!("Optimized runtime:  {} ms", optimized_results.total_runtime);
    println!("FIFO avg CPU:       {:.2}%", fifo_results.avg_cpu);
    println!("Optimized avg CPU:  {:.2}%", optimized_results.avg_cpu);
}

struct SimulationResults {
    total_runtime: u128,
    avg_cpu: f64,
}

fn run_simulation(name: &str, policy: Policy, io_percent: u32) -> SimulationResults {
    println!();
    println!("== {} ==", name);
    println!(
        "{} tasks, {}% IO / {}% CPU, {} workers, cap {}%",
        TOTAL_TASKS,
        io_percent,
        100 - io_percent,
        WORKER_COUNT,
        CPU_LIMIT
    );

    let start = Instant::now();

    let state = Arc::new(Mutex::new(SystemState {
        current_cpu: 0,
        active_workers: 0,
        done: false,
    }));

    let (manager_tx, manager_rx) = mpsc::channel::<ManagerMessage>();

    let mut worker_senders = Vec::new();
    let mut worker_handles = Vec::new();

    for worker_id in 0..WORKER_COUNT {
        let (worker_tx, worker_rx) = mpsc::channel::<WorkerMessage>();
        worker_senders.push(worker_tx);

        let manager_tx_clone = manager_tx.clone();

        let handle = thread::spawn(move || {
            worker_thread(worker_id, worker_rx, manager_tx_clone, start);
        });

        worker_handles.push(handle);
    }

    let generator_tx = manager_tx.clone();
    let generator_handle = thread::spawn(move || {
        generator_thread(generator_tx, start, io_percent);
    });

    let monitor_state = Arc::clone(&state);
    let monitor_name = name.replace(" ", "_").to_lowercase();
    let monitor_handle = thread::spawn(move || {
        monitor_thread(monitor_state, monitor_name);
    });

    let manager_state = Arc::clone(&state);
    let completed = manager_thread(
        manager_rx,
        worker_senders,
        manager_state,
        policy,
    );

    generator_handle.join().unwrap();

    for handle in worker_handles {
        handle.join().unwrap();
    }

    monitor_handle.join().unwrap();

    print_results(&completed, start)
}

fn generator_thread(
    manager_tx: mpsc::Sender<ManagerMessage>,
    start: Instant,
    io_percent: u32,
) {
    let mut rng = StdRng::seed_from_u64(12345);

    for id in 0..TOTAL_TASKS {
        let random_number = rng.gen_range(0..100);

        let kind = if random_number < io_percent {
            TaskKind::IO
        } else {
            TaskKind::CPU
        };

        let cpu_cost = match kind {
            TaskKind::IO => 10,
            TaskKind::CPU => 35,
        };

        let task = Task {
            id,
            arrival_time: start.elapsed().as_millis(),
            kind,
            duration_ms: TASK_DURATION_MS,
            cpu_cost,
        };

        manager_tx.send(ManagerMessage::NewTask(task)).unwrap();

        thread::sleep(Duration::from_millis(ARRIVAL_INTERVAL_MS));
    }

    manager_tx.send(ManagerMessage::GeneratorDone).unwrap();
}

fn manager_thread(
    manager_rx: mpsc::Receiver<ManagerMessage>,
    worker_senders: Vec<mpsc::Sender<WorkerMessage>>,
    state: Arc<Mutex<SystemState>>,
    policy: Policy,
) -> Vec<CompletedTask> {
    let mut fifo_queue: VecDeque<Task> = VecDeque::new();
    let mut io_queue: VecDeque<Task> = VecDeque::new();
    let mut cpu_queue: VecDeque<Task> = VecDeque::new();

    let mut worker_available = vec![true; WORKER_COUNT];
    let mut generator_done = false;
    let mut completed_tasks = Vec::new();

    loop {
        while let Ok(message) = manager_rx.try_recv() {
            match message {
                ManagerMessage::NewTask(task) => match policy {
                    Policy::Fifo => fifo_queue.push_back(task),
                    Policy::Optimized => match task.kind {
                        TaskKind::IO => io_queue.push_back(task),
                        TaskKind::CPU => cpu_queue.push_back(task),
                    },
                },

                ManagerMessage::WorkerDone(done_task) => {
                    {
                        let mut data = state.lock().unwrap();
                        data.current_cpu -= done_task.task.cpu_cost;
                        data.active_workers -= 1;
                    }

                    worker_available[done_task.task.id % WORKER_COUNT] = true;
                    completed_tasks.push(done_task);
                }

                ManagerMessage::GeneratorDone => {
                    generator_done = true;
                }
            }
        }

        for worker_id in 0..WORKER_COUNT {
            if worker_available[worker_id] {
                let next_task = match policy {
                    Policy::Fifo => choose_fifo_task(&mut fifo_queue, &state),
                    Policy::Optimized => choose_optimized_task(&mut io_queue, &mut cpu_queue, &state),
                };

                if let Some(task) = next_task {
                    worker_available[worker_id] = false;

                    {
                        let mut data = state.lock().unwrap();
                        data.current_cpu += task.cpu_cost;
                        data.active_workers += 1;
                    }

                    worker_senders[worker_id]
                        .send(WorkerMessage::Run(task))
                        .unwrap();
                }
            }
        }

        let queues_empty = match policy {
            Policy::Fifo => fifo_queue.is_empty(),
            Policy::Optimized => io_queue.is_empty() && cpu_queue.is_empty(),
        };

        if generator_done && queues_empty && completed_tasks.len() == TOTAL_TASKS {
            for sender in worker_senders {
                sender.send(WorkerMessage::Shutdown).unwrap();
            }

            let mut data = state.lock().unwrap();
            data.done = true;
            break;
        }

        thread::sleep(Duration::from_millis(1));
    }

    completed_tasks
}

fn choose_fifo_task(
    queue: &mut VecDeque<Task>,
    state: &Arc<Mutex<SystemState>>,
) -> Option<Task> {
    if let Some(task) = queue.front() {
        let data = state.lock().unwrap();

        if data.current_cpu + task.cpu_cost <= CPU_LIMIT {
            return queue.pop_front();
        }
    }

    None
}

fn choose_optimized_task(
    io_queue: &mut VecDeque<Task>,
    cpu_queue: &mut VecDeque<Task>,
    state: &Arc<Mutex<SystemState>>,
) -> Option<Task> {
    let current_cpu = {
        let data = state.lock().unwrap();
        data.current_cpu
    };

    // Try CPU first if it fits.
    // This prevents CPU-heavy tasks from waiting until the very end.
    if let Some(task) = cpu_queue.front() {
        if current_cpu + task.cpu_cost <= CPU_LIMIT {
            return cpu_queue.pop_front();
        }
    }

    // If CPU cannot fit, try IO because it only costs 10%.
    if let Some(task) = io_queue.front() {
        if current_cpu + task.cpu_cost <= CPU_LIMIT {
            return io_queue.pop_front();
        }
    }

    None
}

fn worker_thread(
    worker_id: usize,
    worker_rx: mpsc::Receiver<WorkerMessage>,
    manager_tx: mpsc::Sender<ManagerMessage>,
    start: Instant,
) {
    loop {
        let message = worker_rx.recv().unwrap();

        match message {
            WorkerMessage::Run(task) => {
                let start_time = start.elapsed().as_millis();

                thread::sleep(Duration::from_millis(task.duration_ms));

                let finish_time = start.elapsed().as_millis();

                let completed = CompletedTask {
                    task,
                    start_time,
                    finish_time,
                };

                manager_tx.send(ManagerMessage::WorkerDone(completed)).unwrap();

                // println!("Worker {} finished a task", worker_id);
            }

            WorkerMessage::Shutdown => {
                break;
            }
        }
    }
}

fn monitor_thread(state: Arc<Mutex<SystemState>>, name: String) {
    let file_name = format!("{}_monitor_log.csv", name);
    let mut file = File::create(&file_name).unwrap();

    writeln!(file, "sample,cpu_usage,active_workers").unwrap();

    let mut sample = 0;

    loop {
        let data = state.lock().unwrap();

        writeln!(
            file,
            "{},{},{}",
            sample, data.current_cpu, data.active_workers
        )
        .unwrap();

        if data.done {
            break;
        }

        drop(data);

        sample += 1;
        thread::sleep(Duration::from_millis(10));
    }

    println!("monitor csv: {}", file_name);
}

fn print_results(completed: &Vec<CompletedTask>, start: Instant) -> SimulationResults {
    let total_runtime = start.elapsed().as_millis();

    let mut total_wait = 0;
    let mut total_turnaround = 0;
    let mut max_wait = 0;
    let mut max_wait_id = 0;

    let mut io_count = 0;
    let mut cpu_count = 0;

    for item in completed {
        let wait_time = item.start_time - item.task.arrival_time;
        let turnaround_time = item.finish_time - item.task.arrival_time;

        total_wait += wait_time;
        total_turnaround += turnaround_time;

        if wait_time > max_wait {
            max_wait = wait_time;
            max_wait_id = item.task.id;
        }

        match item.task.kind {
            TaskKind::IO => io_count += 1,
            TaskKind::CPU => cpu_count += 1,
        }
    }

    let total = completed.len() as u128;

    let avg_wait = total_wait as f64 / total as f64;
    let avg_turnaround = total_turnaround as f64 / total as f64;

    println!();
    println!("— results —");
    println!("total runtime          : {} ms", total_runtime);
    println!("tasks completed        : {} (IO={}, CPU={})", completed.len(), io_count, cpu_count);
    println!("avg wait time          : {:.2} ms", avg_wait);
    println!("avg turnaround time    : {:.2} ms", avg_turnaround);
    println!("max wait time          : {} ms (task #{})", max_wait, max_wait_id);

    SimulationResults {
        total_runtime,
        avg_cpu: 0.0,
    }
}