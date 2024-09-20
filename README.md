# Chord Conductor
this project consists of three pieces:

1. a distributed hash table implemented with the [chord protocol](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf)
2. a "conductor" websocket server, which runs simulations of chord systems with various parameters
3. a client for the conductor, which visualizes the system and provides some basic controls

it's far from a complete production ready system, but i hope it shows some breadth and depth of understanding in this domain

## Usage
to run the conductor, first build both the conductor and chord node binaries with
```bash
cargo build
```

then run the conductor with
```bash
cargo run --bin conductor
```

the client can be found in the `client` folder, and can be run with
```bash
bun run dev
```
the client will be on `localhost:4321`

## Demo
## Implementation Details
#### chord node
the "node" in this project is a gRPC server that implements a distributed hash table on top of the chord protocol.
the chord protocol is a method for knowing which node has a piece of data, based on it's contents. it works by running a universal hashing function, like `sha1` or `sha256`
on both the data and a unique piece of information about the nodes (typically the ip address). the nodes are then placed on an ordered "ring" based on their hash, and are responsible
for data that hashes between their predecessor and themselves.

[chord image]

the rest of the details about the chord protocol can be found in the [paper](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf) as well as the code. for this section, i want to focus on a couple of technical decisions.

**use of `tokio::sync::watch`**

early versions of this project used reader-writer locks for all data syncronization. with this, i ran into some distributed deadlock problems, particularly with updating the nodes successors and predecessors. while it is absolutely possible to use locks for this and not have deadlock problems, i decided to go a different route.

the `tokio::sync::watch` module provides a lock free way for tasks set a value, and read the most recent value for a piece of data. because the gRPC connections for the successors are cheap to clone, this allows all of the proceedures in the node to get the most up to date values for this data by borrowing and cloning, without starving or waiting for writers. while this does mean that a successor or predecessor can change while a procedure is holding and old value, the chord system *expects* these values to be wrong sometimes, and includes numerous processes for dealing with this case, namely the stabilize and fix fingers proceedures. at least for successor and predecessor data, locks are not necessary or intuitive for chord systems.

**`BTreeMap` over `HashMap`**

most chord based distributed hashtable implementations that i've seen use a standard hash map. given the ordered nature of the chord system, an ordered map like `std::collections::BTreeMap` seemed like a much more natural fit. while it is true that search complexity moves from constant time to logarithmic time with this change, the latency for gets and sets in a chord system is dominated by network calls between nodes. crucialy, an ordered map allows for fast spliting and appending of large sections of data, which is necessary for moving data between nodes as nodes join or leave the system. so the proposed trade off here is to potentially loose a bit of performance on gets and sets, while gaining robustness, as nodes will be able to handle much more erratic behavior from their neighbors

it's important to note that this isn't something i've actually extensively profiled, which is something that i plan to do once the nodes have been deployed. i also want to explore potentially using a skip-list based ordered map like [`crossbeam_skiplist`](https://docs.rs/crossbeam-skiplist/latest/crossbeam_skiplist/) for a completely lock-free node implementation.

#### conductor
the conductor is a websocket server that runs chord simulations. it spawns nodes, and simulates get and set activity on them. the conductor polls the nodes for their current state, and keeps track of various statistics of each simulation, and sends all of this information to the client. simulations can be started with various different configurations, for testing how the chord system responds to different conditions and use cases. the data used for the simulation is a collection of quotes, this isn't necessarily a realistic dataset for this type of system, but it works well enough for seeing how the hash table is working.

#### conductor client
the client for the conductor is a relatively simple react app. it allows for choosing the starting parameters for the conductor simulations, as well as displays a visual of the graph alongside some basic statistics on the system. the graph is rerendered every time the client gets new poll data from the conductor, which is wasteful, but was necessary to display the graph well as the graph rendering library (Sigma.js) that the client uses didn't have a good way to ensure that the nodes stayed in order when the graph was modified instead of re-rendered. probably the best solution would be to implement a custom purpose built rendering solution that can handle all the nuances of the chord system.


## Next Steps 
**deployment:**

the first and obvious next step for this project is to actually deploy the chord nodes, and potentially the conductor as well.
right now, the max number of nodes the system runs is 50, which is an unrealisticly small number for a real system.
each node as well as the conductor have a thread pool equal to the number of cpu cores (this is the tokio runtime default), and all of them are trying to operate concurrently.
this means that, even on a beefy machine with the small number of nodes supported, the simulations are often bottlenecked by operating system scheduling,
and it is difficult to get realistic behavior from the nodes as only a subset of them are ever running simultaniously.

deploying the system would allow for both drastically increasing the number of nodes in the chord ring, as well as simulating more realistic concurrent behaviour.

**supporting node leaves and failures**

currently, the implementation does not support nodes leaving the chord ring, voluntarily or through node failure.
a crucial aspect of the chord protocol is to handle these events correctly in a performant way, and leaving it out doesn't show the full capabilities of the system.

there are also some practical advantages to implementing this behaviour. it would provide the ability to change the number of nodes in circulation live without issues,
as well as the potential for things like partitioning off a subset of an active system for testing purposes.

**using tracing for data collection**

currently, the conductor works by polling the nodes for information about their current state.
a much more practical implementation would be to use something like the [`tracing`](https://docs.rs/tracing/latest/tracing/) crate.
nodes could include a small endpoint that implements the `Subscriber` trait and sends logs to the conductor over sse or websockets.

this would allow for the conductor to be useful for active production systems, as well as open up the possibility for more serious profiling and post-test reports
