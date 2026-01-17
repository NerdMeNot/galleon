# Dockerfile for Galleon benchmarks
# Supports both Docker and Podman
# Includes Go, Zig, Python with Polars and Pandas

FROM golang:1.25-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    wget \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Zig 0.15.2 (stable release)
ARG ZIG_VERSION=0.15.2
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        ZIG_ARCH="aarch64"; \
    else \
        ZIG_ARCH="x86_64"; \
    fi && \
    ZIG_FILE="zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" && \
    echo "Downloading Zig: $ZIG_FILE" && \
    wget -q https://ziglang.org/download/${ZIG_VERSION}/${ZIG_FILE} && \
    tar -xf ${ZIG_FILE} && \
    mv zig-${ZIG_ARCH}-linux-${ZIG_VERSION} /opt/zig && \
    rm ${ZIG_FILE}

ENV PATH="/opt/zig:${PATH}"

# Create Python virtual environment and install dependencies
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
RUN pip install --no-cache-dir numpy polars pandas

# Set working directory
WORKDIR /galleon

# Copy the entire project
COPY . .

# Build Zig library
RUN cd core && zig build -Doptimize=ReleaseFast

# Default command: run comprehensive benchmark comparison
CMD ["python3", "/galleon/go/benchmarks/benchmark_comparison.py"]
