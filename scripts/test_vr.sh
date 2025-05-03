#!/bin/bash
cd "$(dirname "$0")/.."
zig build test -Doptimize=Debug -- viewstamped_replication
