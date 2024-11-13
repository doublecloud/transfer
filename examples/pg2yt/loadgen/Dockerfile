# Start from the official Golang base image
FROM golang:1.22-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the Go application
RUN go build -o crud-load-gen

# Run the Go application
CMD ["./crud-load-gen"]
