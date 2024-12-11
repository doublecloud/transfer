# Stage 1: Build Go binary
FROM golang:1.22-alpine AS builder

# Set up environment variables for Go
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Set the working directory
WORKDIR /app

# Copy the Go modules files
COPY go.mod go.sum ./

# Download Go module dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the Go binary
RUN go build -o /app/trcli ./cmd/trcli/*.go

# Stage 2: Base image setup (use Ubuntu for the other tools and dependencies)
FROM ubuntu:jammy

ENV TZ=Etc/UTC

ENV ROTATION_TZ=Etc/UTC

ENV DEBIAN_FRONTEND=noninteractive

RUN echo $TZ > /etc/timezone && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

# Install required packages
RUN DEBIAN_FRONTEND=noninteractive apt-get update -qq && \
    apt-get install -y gnupg wget lsb-release curl ca-certificates openssh-client \
    iptables supervisor apt-transport-https dirmngr nano vim telnet less tcpdump net-tools \
    tzdata lsof libaio1 unzip git && \
    # Add PostgreSQL official repository
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update -qq && \
    apt-get install -y postgresql-client-16 && \
    rm -rf /var/lib/apt/lists/*

# Pre-configure debconf to avoid ClickHouse prompts (password or confirmation)
RUN echo "clickhouse-client clickhouse-server/root_password password root" | debconf-set-selections && \
    echo "clickhouse-client clickhouse-server/root_password_again password root" | debconf-set-selections

# Install ClickHouse
RUN DEBIAN_FRONTEND=noninteractive apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754 && \
    echo "deb https://packages.clickhouse.com/deb stable main" >> /etc/apt/sources.list && \
    apt-get update -qq && \
    apt-get install -y clickhouse-server=23.5.3.24 clickhouse-client=23.5.3.24 clickhouse-common-static=23.5.3.24 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user and group
RUN addgroup --system trcligroup && adduser --system --ingroup trcligroup trcliuser

# Copy the Go binary from Stage 1 (builder)
COPY --from=builder /app/trcli /usr/local/bin/trcli

RUN chmod +x /usr/local/bin/trcli

# Set ownership of the binary to the non-root user
RUN chown trcliuser:trcligroup /usr/local/bin/trcli

# Switch to the non-root user
USER trcliuser

ENTRYPOINT ["/usr/local/bin/trcli"]
