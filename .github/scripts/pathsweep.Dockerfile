# What .github/scripts/pathsweep.py needs, for running it on a machine that is
# not Linux. See the script for how to build and run it.
FROM rust:1
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 nodejs npm python3 \
    && rm -rf /var/lib/apt/lists/*
