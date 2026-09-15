# Seventh State Classic-b-Gone Plugin

This plugin intercepts calls to queue.declare to prevent classic queues from being declared.

(c) Seventh State 2026

## Build

This will generate the plugin file in plugins/

```bash
gmake dist DIST_AS_EZS=1 
```

## Tests

```bash
gmake tests
```

## Build and Test Locally with Docker Compose

You can build and test the plugin in a consistent environment using Docker Compose.

1. **Build the Docker image:**
   ```bash
   docker compose -f build/docker-compose.yml build --no-cache
   ```

2. **Run tests and build the plugin:**
   ```bash
   docker compose -f build/docker-compose.yml run test-and-build make tests
   docker compose -f build/docker-compose.yml run test-and-build make dist
   ```

3. **Test logs and build artifacts** will be available in your project root directory after the run.

   - **logs/**: This directory contains all test logs generated during the run.
   - **plugins/**: This directory contains the built plugin `.ez` files.


