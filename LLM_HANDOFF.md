# BlazingMQ Rust SDK learning handoff

Last updated: 2026-10-21.

This is a structured summary of the conversation, not a verbatim chat export. It preserves the goals, decisions, explanations, current implementation, and next steps needed to continue tutoring.

## Instructions from the user — read first

- The user is learning Rust by implementing a BlazingMQ SDK themselves.
- **Do not edit their source code. Do not provide solution code unless explicitly requested.** Explain concepts, give small steps, and review their attempts by reading the actual files.
- The user explicitly authorized writing this Markdown handoff into the SDK repository. This is not permission to change implementation files.
- Requests such as “like so?”, “now?”, and “check my code” mean read the current saved code and review it. Do not assume it still matches a previous version.
- Keep explanations concise, concrete, and focused on the immediate question. Explain how a step contributes to the final SDK.
- Avoid repeatedly introducing new structs, abstractions, or changing architecture without explaining why. This caused confusion and frustration earlier.
- The user wants to retain the current separate Reader, Writer, and Coordinator design. Cloneability and incoming-receiver API ownership are intentionally deferred.
- Do not confuse inspecting code with compiling/testing it. State precisely which was done.
- Early in the conversation the assistant edited source without sufficient understanding of the user's teaching intent. The user corrected this firmly. Do not repeat that mistake.

## Repositories

- Original exercise: `/Users/shengy/repos/ruststuff/practice-statemachine-types`.
- Current SDK exercise: `/Users/shengy/repos/ruststuff/bmqsdk-tryout`.
- Earlier messages called it `../bmqsdk`; that path later ceased to exist. The current project is `bmqsdk-tryout`.
- Implementation is currently all in `src/main.rs`, a binary crate. This is intentional. Modules and migration to `lib.rs` are deferred until useful behavior is established.
- Cargo package is `bmqsdk-tryout`, Rust edition 2024. Tokio dependency was `1.53.2` with `full` features. Check the actual manifest and lockfile before relying on this.

## Objective and latest stopping point

Build a Rust BlazingMQ SDK using Tokio, while teaching the user to write it.

The transport exercise and connection-setup refactor are complete for the normal peer-disconnect path. The user asked whether the protocol is next. The agreed next milestone is:

**Inspect official BlazingMQ protocol definitions and an existing client, then connect to a real broker and complete its handshake.**

No BlazingMQ protocol research or implementation has been done yet in this conversation. Do not invent wire layouts, handshake states, request identifiers, or supported codecs. Research primary sources first and teach one exchange at a time.

The current literal `handshake from client` is just test text, not a protocol message.

## Current architecture

Sending:

Application -> Client -> bounded channel -> Coordinator -> bounded channel -> Writer -> TCP -> broker

Receiving:

Broker -> TCP -> Reader -> bounded channel -> Coordinator -> bounded channel -> application receiver

There are three spawned tasks:

- Reader owns `OwnedReadHalf`, reads byte chunks, and forwards them.
- Writer owns `OwnedWriteHalf`, receives outgoing byte vectors, and writes each completely.
- Coordinator owns channel endpoints and routes data. Eventually it owns the protocol state machine.

Client is currently the application's sending handle plus owner of all three task JoinHandles. It is not itself a spawned task.

Separate tasks do not require separate structs. The structs organize behavior and ownership; `tokio::spawn` creates concurrency. We considered combining everything in the coordinator, but retained the current design. Awaiting a complete write in a coordinator branch prevents that coordinator from handling other events during the wait. Separate tasks isolate network I/O, although bounded-channel sends inside coordinator branches can still delay its loop.

## Current implementation, as last inspected

### Reader

Fields:

- `from_tcp_rx: OwnedReadHalf`
- `to_coordinator_tx: tokio::sync::mpsc::Sender<Vec<u8>>`

`run(mut self)` returns `Result<(), Box<dyn Error + Send + Sync>>`.

It repeatedly reads into a 10-byte buffer. Zero bytes means EOF: prints `no more`, then returns success. Positive reads copy only `buffer[..bytes_read]` to a vector, send it to coordinator, and also print bytes for debugging. Read/send errors propagate.

These are TCP byte chunks, not framed protocol messages. A single read can contain part of a message or pieces of multiple messages.

### Writer

Fields:

- `tcp_tx: OwnedWriteHalf`
- `ch_rx: mpsc::Receiver<Vec<u8>>`

`run(mut self)` loops over channel payloads. For each payload it resets an offset, then:

1. Awaits socket writability.
2. Calls `try_write` on the remaining payload slice.
3. Advances offset on positive byte count.
4. Retries after `WouldBlock`.
5. Returns a boxed I/O `WriteZero` error on zero while bytes remain.
6. Propagates other errors.

It exits after channel closure and drain. Dropping the write half shuts down the TCP sending direction. The writer originally sent one hardcoded greeting; it is now persistent and channel-driven.

### Coordinator

Fields:

- `from_client_rx: mpsc::Receiver<Vec<u8>>`
- `to_writer_tx: mpsc::Sender<Vec<u8>>`
- `from_reader_rx: mpsc::Receiver<Vec<u8>>`
- `to_client_tx: mpsc::Sender<Vec<u8>>`

`run(mut self)` loops around `tokio::select!` with one receive operation per input channel.

- Client `Some(payload)`: forwards to writer.
- Reader `Some(payload)`: debug prints and forwards to application.
- Either input returns `None`: returns success.

This is a **temporary shutdown policy**. Exiting on either input closure can discard pending data from the other input. It is not final robust SDK lifecycle behavior.

### Client

Fields:

- `to_coordinator_tx: mpsc::Sender<Vec<u8>>`
- `reader_handle`, `coordinator_handle`, `writer_handle`, each of type `JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>`

Methods:

- `send(&self, payload: Vec<u8>)`: awaits channel send and returns a boxed-error Result. Success means accepted into the coordinator channel, **not written to TCP and not broker-confirmed**.
- `connect(address: &str)`: async associated function returning `Result<(Self, mpsc::Receiver<Vec<u8>>), Box<dyn Error + Send + Sync>>`. It connects TCP, creates four bounded channels of capacity 32, splits TCP, spawns the three tasks, then constructs Client with its sender and all handles. Returns Client plus the incoming application receiver. It does not await task completion.
- `wait_close(self)`: awaits reader, coordinator, writer sequentially, checking the outer JoinError and inner operation Result for each, then returns success.

`wait_close` consumes Client. It **waits for closure; it does not initiate closure**. Sequential `?` can return early and leave remaining task handles unawaited if an error occurs. This limitation has been explicitly acknowledged, not fixed.

### Main

1. Calls `Client::connect("127.0.0.1:5555").await`.
2. Sends the test greeting as an owned vector.
3. Loops on the returned mutable incoming receiver, printing payloads.
4. Breaks on `None` (does not return prematurely).
5. Calls `client.wait_close().await` and propagates errors.
6. Returns `Ok(())`.

The last revision removed an unused synchronous `std::sync::mpsc::Receiver` import. That latest refactor was reviewed by inspection, not compiled or runtime tested.

## Testing already performed

Before the Client::connect / wait_close refactor, the transport passed an actual build and four local integration scenarios:

- Empty incoming stream followed by orderly EOF.
- Six-byte payload.
- Nineteen-byte payload sent in separated chunks.
- 65,536 bytes, exceeding both the 10-byte buffer and channel capacities.

Each verified the exact outgoing greeting, reconstructed incoming bytes from application output, and successful process shutdown after peer EOF. Tests did not assume TCP read boundaries match sends.

Build command used `cargo build --locked --offline --target-dir /tmp/bmqsdk-review-target`. A temporary Python TCP server listened on localhost port 5555 and ran the binary, with bounded timeouts. Opening the listening socket required sandbox escalation. No source was edited and no permanent test file was added.

These tests cover normal transport operation, not comprehensive failure handling, protocol behavior, or the later connection refactor. Do not describe the latest code as tested unless you actually test it.

The user also manually tested with `nc -l 5555` and `cargo run`.

## API decisions and deferred issues

- The application should not construct internal tasks or channels. Connection setup is now inside Client::connect.
- An eventual protocol-aware connect should likely return only after a successful handshake, but today's connect only establishes TCP.
- State belongs in Coordinator; the other tasks handle transport.
- Adding commands does **not** require another permanent client-to-coordinator channel. Change its message type from Vec<u8> to a Command enum.
- Per-request replies can use a oneshot sender carried in a command, as in the traffic-light exercise.
- Reader events eventually carry decoded protocol messages, EOF, and failures. Responses must be matched using actual protocol rules, not assumed IDs.
- Unsolicited application messages use the incoming receiver.
- Receiver ownership remains provisional. The user feels receiving belongs in Client. This is valid; separation was proposed for cloning, not mandated.
- A Tokio mpsc receiver is not Clone. A sender-only Client could derive Clone, but Rust does not do this automatically. Current Client also owns JoinHandles, which are not cloneable.
- We briefly proposed an Incoming wrapper, but the user felt there were too many structs. Do not reintroduce it unnecessarily. Plain receiver is currently used.
- Robust shutdown/failure supervision, explicit close, cancellation, task panic handling, bounded-channel backpressure policies, framing, and protocol correctness remain future work.
- Do not claim wait_close guarantees all tasks are joined on errors, or that dropping Client synchronously waits for tasks.

## Concepts the user has been learning

### Futures and tasks

- Calling an async method creates a future; its body does not execute just because it contains await.
- Awaiting it drives it toward completion and yields its output.
- `tokio::spawn` accepts a future, schedules it as a task, and returns a JoinHandle immediately.
- A spawned task is polled independently of whether main awaits its handle.
- Awaiting a handle waits for the whole task to finish, not for one read/message.
- Two tasks run concurrently even when main awaits their handles sequentially.
- Main can await an application receiver without spawning another task. The three internal tasks continue running.
- If main needs independent keyboard input and receiving, select or another task may be appropriate.
- An async block ending in `reader.run()` can return an unexecuted inner future; `reader.run().await` performs that work and returns its Result.
- With a borrowed `run(&mut self)`, returning its future from a block containing a local object also causes a lifetime error.
- A semicolon can discard a future, leading to warnings and no work.

### Nested results and errors

- JoinHandle has **one** generic parameter: task output.
- Task output here is `Result<(), Box<dyn Error + Send + Sync>>`.
- Awaiting the handle yields an outer Result with JoinError around that inner Result.
- First `?` handles task completion; second handles operation failure. Spawn does not flatten results.
- `Send` and `Sync` are marker traits, generally automatically derived from contents. std::io::Error already satisfies them.
- Boxing with `.into()` does not add thread-safety traits. It converts an already suitable concrete error to the requested boxed trait object.
- ErrorKind::WriteZero is a category; construct std::io::Error from it before boxing.
- `Result<(Client, Receiver), Error>` represents two success values or one failure.
- `recv().await` on mpsc returns Option, not Result.
- `Ok(())` creates a value; `return Ok(())` exits a function. `break` exits only the loop, allowing cleanup below.

### Ownership and channels

- `into_split` produces two owned halves of one TCP connection; it does not spawn tasks.
- `mut self` transfers ownership and permits mutable field access. `&mut self` borrows. Awaiting a JoinHandle by value consumes it; a shared `&self` cannot move it out.
- Sender clones share one channel. New channel creation produces a new independent channel.
- Calling traffic-light spawn_client twice creates two workers and independent states; cloning one remote shares its worker/state.
- Channel closure and TCP closure are different events.
- Dropping all senders lets a receiver loop finish after draining queued messages. It does not wait for that completion.

### select and write loops

- select must be inside a loop for repeated processing.
- One recv per receiver in a select. Two simultaneous recv futures on the same receiver conflict via mutable borrowing.
- Bind full Option output, then distinguish Some and None inside the selected branch.
- Matching only Some disables that branch when None arrives; all disabled branches with no else can panic.
- try_write can return partial success or WouldBlock after readiness. Track offset, retry readiness, and handle WriteZero.
- Only print/copy the filled buffer slice. to_vec already copies; a subsequent clone is unnecessary.

## Original traffic-light exercise

The user first implemented a typed traffic-light state machine controlled through a remote and channels. Two timing issues were discussed:

1. Success reply must follow executing the transition and storing the new state. Reply-before-transition only acknowledges acceptance and can race program exit. Invalid transitions preserve the state and return an error after storing it.
2. Last command completion is not worker completion. Drop all remotes, then join/await the worker before main exits so its final print and return happen. “worker done” is more accurate than “all threads done.”

This established the conceptual pattern reused here: one task owns state, remotes send commands, replies arrive over oneshots, and handles establish task completion.

## Recommended continuation

The user last agreed the protocol is next. Avoid another architecture detour. Acknowledge the working transport foundation and inspect official BlazingMQ sources for:

1. Message framing: length/header, boundaries, limits, encoding.
2. Initial connection/negotiation exchange and expected broker response.
3. What establishes readiness and failure.

Explain the exchange and derive the first coordinator states from it. Set a narrow milestone: complete one real broker handshake. Do not label the existing arbitrary-byte exercise a real SDK handshake.

Before further tests, read the current saved code. Keep tutoring steps small and use the user's implementation rather than supplying a complete solution.
