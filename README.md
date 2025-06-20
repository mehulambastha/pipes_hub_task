# Order Management System (OMS)

This is a lightweight, production-leaning implementation of an Order Management System in Python. It simulates how a trading OMS would handle:

- Incoming orders (new, modify, cancel)
- Exchange communication
- Throttling and queuing
- Session-based trading
- Response tracking and latency logging
- Persistence of trade responses

It uses threads, some locking, and tries to be reasonably robust and testable — but still just code you can read in one sitting.

---

## Features

- [*] Time-based trading hours (e.g., only allow orders from 9 AM to 5 PM)
- [*] Order throttling (e.g., max 100 orders per second)
- [*] In-memory queue for backlogged orders
- [*] Modify/cancel orders in queue
- [*] Thread-safe tracking of sent orders and their responses
- [*] SQLite persistence of responses with latency logging
- [*] Background threads for session and order processing
- [*] Graceful shutdown with SIGINT/SIGTERM

---

## Running the System

```bash
python order_management.py
````

The main loop sends some fake orders for testing. You'll see logs, order dispatches, and queue behavior.

---

## Running the Tests

```bash
python tests.py
```

The test suite covers:

* Order validation
* Queue behavior
* Throttling logic
* Trading session logic
* Tracker and persistence

---

##  Requirements

Only uses the Python standard library:

```txt
sqlite3
threading
queue
logging
time
datetime
enum
dataclasses
```

No pip install required.

---

##  Project Structure

```
order_management.py   # Main OMS logic
tests.py              # Tests
order_management.db   # SQLite file generated on first run
order_management.log  # Log file
```
---

##  Disclaimer

This is not production-grade. No real networking, auth, or exchange protocols are implemented. It's more of a solid base, testable and understandable.

# Design Document - Order Management System

This document gives a rough breakdown of how the OMS works, why certain things were done that way, and how it impacts things like robustness or scalability.

---

##  Core Design Philosophy

- Keep things simple and single-threaded where possible.
- Use `threading` + `queue` for concurrency, not async/event loops.
- Build realistic constraints (throttling, trading hours).
- Treat orders like first-class citizens: track them, persist them, modify/cancel them.
- Code should be readable over clever.

---

## Key Components

### `OrderRequest` / `OrderResponse`
- Simple dataclasses to represent inbound/outbound orders.
- `__post_init__` does basic validation (e.g. price > 0).
- Enums are used to make types clearer (`RequestType`, `ResponseType`).

---

### `OrderQueue`
- In-memory dict-based queue.
- Supports `modify_order`, `cancel_order`, `get_next_order` (FIFO via timestamps).
- Uses a lock for thread-safety.
- Could be swapped out with Redis if scaling across processes.

---

### `OrderThrottler`
- Limits number of orders sent per second.
- If limit is hit, orders are queued (via `OrderQueue`).
- Uses a simple time window (resets counter every second).
- Not token-bucket or leaky-bucket — simple and effective for one thread.

---

### `OrderTracker`
- Tracks sent orders and their timestamps.
- When a response comes back, calculates round-trip latency.
- Uses lock for thread-safe tracking.
- Helps us monitor "how fast are we" without having metrics infra.

---

### `PersistentStorage`
- SQLite-based.
- Stores all responses along with latency in `order_responses` table.
- Every insert is done via a lock to avoid DB conflicts.
- This makes the system *resumable* and auditable in a basic way.

---

### `TradingSession`
- Keeps track of whether we're inside trading hours or not.
- Also tracks whether we're logged in or not.
- Used by session thread to determine when to logon/logout.

---

### `OrderManagement`
- Orchestrator class.
- Spawns background threads:
  - One for managing trading session (logon/logout)
  - One for draining the queue and sending orders
- Handles `on_data()` for inbound order requests
- Handles `on_data_response()` for simulated exchange responses
- Provides `get_status()` for monitoring/debugging
- Holds references to all other core modules

---

## Concurrency Design

- Uses threads for:
  - Background session management
  - Background order sending
  - Test cases / simulation
- Shared state (queue, tracker, storage) is protected via locks.
- There’s no `async` or multiprocessing — just Python threads with care taken.

---

## Scalability Thoughts

### What helps:
- Order throttler + queue means bursty traffic won’t overwhelm the system
- SQLite gives us persistent response logs without needing infra
- Separate session and order processors isolate concerns

### What doesn’t scale:
- SQLite won’t scale beyond one instance/thread well
- `queue.Queue` and shared data will bottleneck in multi-process scenarios
- Python GIL limits parallelism (but fine for prototyping or single-core apps)
- No retry/backoff/recovery logic yet

---

## Robustness Thoughts

### What helps:
- Logging everywhere (both to file and stdout)
- Graceful shutdown with signal handling
- Exception handling in background threads
- Validations on incoming orders (price, side, qty)

### What I  could add next:
- Retry logic for failed DB writes
- Configurable trading windows, symbols, etc.
- Real exchange simulation or mock network layer
- Health check API or CLI monitor

---

## TL;DR

It’s a small, modular OMS that can:
- Accept orders
- Queue them
- Throttle sending
- Track responses
- Persist results
- Work inside specific trading windows

It’s not perfect, but it’s tight, readable, and solid enough to grow.
