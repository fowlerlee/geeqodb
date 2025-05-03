# GeeqoDB Deterministic Simulation Testing Framework

This directory contains a deterministic simulation testing framework for GeeqoDB, inspired by TigerBeetle's approach to testing distributed systems. The framework allows for controlled, reproducible testing of complex distributed system behaviors, including network partitions, node failures, and other fault scenarios.

## Overview

The simulation framework consists of several key components:

1. **Scheduler**: A deterministic scheduler that controls the execution order of tasks in the simulation.
2. **VirtualClock**: A simulated clock system for time-based operations, including support for clock drift.
3. **SimulatedNetwork**: A network simulation layer for controlled message passing between nodes, with support for network partitions, message delays, and message loss.
4. **SimulatedDisk**: A disk simulation layer for deterministic I/O operations, with support for read/write delays, errors, and data corruption.
5. **Simulation**: The main simulation environment that ties together all simulation components.

## Test Scenarios

The framework includes test scenarios for various distributed system protocols and components:

1. **Viewstamped Replication**: A basic implementation of the Viewstamped Replication protocol, with tests for normal operation, primary failure, and network partitions.

## Running Tests

To run the simulation tests, use the `run_simulation_tests` script:

```bash
zig build run -- -p scripts/run_simulation_tests
```

Or with specific options:

```bash
zig build run -- -p scripts/run_simulation_tests -- --scenario vr_basic --seed 123 --verbose
```

Available options:
- `--scenario, -s <name>`: Run a specific scenario
- `--seed, -r <number>`: Set the random seed (default: 42)
- `--verbose, -v`: Enable verbose output
- `--help, -h`: Show help message

## Adding New Scenarios

To add a new test scenario:

1. Create a new file in the `src/simulation/scenarios` directory.
2. Implement your scenario using the simulation framework.
3. Add your scenario to the `run_simulation_tests.zig` script.

## Design Principles

The simulation framework follows these key design principles:

1. **Determinism**: Given the same seed, the simulation will always produce the same results.
2. **Controlled Chaos**: The framework allows for controlled injection of faults, delays, and other chaos.
3. **Reproducibility**: Test failures can be reproduced by using the same seed.
4. **Isolation**: The simulation runs in a single process, isolated from the real world.

## Future Improvements

Planned improvements to the framework include:

1. More comprehensive test scenarios for GeeqoDB components.
2. Support for more complex fault injection patterns.
3. Visualization tools for simulation analysis.
4. Integration with continuous integration systems.
5. Support for fuzzing and property-based testing.
