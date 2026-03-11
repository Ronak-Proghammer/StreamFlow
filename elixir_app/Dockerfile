# ============================================================
# Elixir App Dockerfile
# ============================================================
# We use a multi-stage build. This is a production best practice:
#
# Stage 1 (builder): Has all build tools, compiles the app
# Stage 2 (runtime): Tiny image with ONLY the compiled release
#
# Result: Instead of a ~1GB image with Erlang/Elixir build tools,
# we get a ~100MB runtime image. Smaller = faster deploys,
# smaller attack surface, less to scan for vulnerabilities.
# ============================================================

# --- Stage 1: Builder ---
# Use the official Elixir image based on Debian Bullseye
FROM elixir:1.15-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
  build-essential \
  git \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install hex (Elixir package manager) and rebar (Erlang build tool)
# --force skips the "are you sure?" prompt
RUN mix local.hex --force && \
    mix local.rebar --force

# Set build environment
ENV MIX_ENV=prod

# Copy dependency files FIRST (before source code).
# Why? Docker caches layers. If we copy source first,
# every code change invalidates the dependency cache.
# This way, deps only re-download when mix.exs changes.
COPY mix.exs ./
RUN mix deps.get --only prod
RUN mix deps.compile

# NOW copy the rest of the source code
COPY . .

# Compile the application
RUN mix compile

# Build a self-contained Elixir release
# A "release" bundles the Erlang VM + your app into one artifact
# No Elixir/Erlang needed on the production machine!
RUN mix release

# --- Stage 2: Runtime ---
# Much smaller base image — just what's needed to RUN the app
FROM debian:bullseye-slim AS runtime

RUN apt-get update && apt-get install -y \
  libssl-dev \
  libncurses5 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy ONLY the compiled release from the builder stage
COPY --from=builder /app/_build/prod/rel/streamflow ./

# Run as non-root user (security best practice)
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# The port our Phoenix app listens on
EXPOSE 4000

# Start the application
# "eval" runs a one-off command in the release
# For long-running servers we use "start" or the foreground command
CMD ["bin/streamflow", "start"]