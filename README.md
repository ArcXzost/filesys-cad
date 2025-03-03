# FileSys-CAS: A Distributed File System with Content Addressable Storage

## Overview

This file system, built on top of Windows File System, manages and retrieves data across a network of nodes. Unlike traditional file systems that rely on hierarchical paths or filenames, CADFS identifies and accesses files based on their **content hash**. This ensures data integrity and efficient retrieval.

### How Content Addressing Works
1. **File Key Hashing**:  
   - When a file is stored, its key is hashed using **SHA-256** to generate a unique identifier. This hash determines the file's location in the distributed network.
   
2. **Path Transformation**:  
   - The file's hash is transformed into a directory structure using a **PathTransformFunc**, ensuring files are stored in a content-addressable manner.

3. **Consistent Hashing for Peer Assignment**:  
   - The **responsible peer** for a file is determined by consistent hashing of the file key. This ensures the same key always maps to the same peer, enabling efficient file retrieval.

4. **File Versioning**:  
   - Each version of a file is identified by a unique `VersionID`, generated using a cryptographic hash. This allows for content-based retrieval of specific versions.

5. **File Integrity Verification**:  
   - The hash of the file content is stored in the metadata, allowing for verification of file integrity during retrieval.

This project implements a peer-to-peer content-addressable file storage system in Go, with a custom network library built on top of TCP.

## Key Features

- **Content Addressable Storage**: Files are identified by their content hash, ensuring data integrity.
- **Distributed Storage**: Files are distributed across multiple peers for redundancy and fault tolerance.
- **Encryption**: Data is encrypted during storage and transmission for security.
- **Versioning**: Supports file versioning, allowing users to track and restore previous versions.
- **Health Monitoring**: Automatically monitors peer health and redistributes files from unhealthy peers.

## Getting Started

### Prerequisites

- **Go 1.22.2** or later: Install from the [official Go website](https://go.dev/doc/install).

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/ArcXzost/filesys-cad.git
   cd filesys-cad
   ```

2. Install dependencies:
   ```bash
   go mod tidy
   ```

3. Build and run the project:
   ```bash
   make run
   ```

## CLI Usage

The system provides an interactive CLI for managing files and peers. Here are the available commands:

### Basic Commands
- **Store a file**:
  ```bash
  dfs upload --path /path/to/file
  ```

- **Retrieve a file**:
  ```bash
  dfs get --key <file_key>
  ```

- **Delete a file**:
  ```bash
  dfs delete --key <file_key>
  ```

- **List all files**:
  ```bash
  dfs list
  ```

### File Versioning

- **List file versions**:
  ```bash
  dfs versions --key <file_key>
  ```

- **Restore a specific version**:
  ```bash
  dfs restore --key <file_key> --version <version_id>
  ```

### Peer Management

- **Start additional peers**:
  ```bash
  dfs start-peers --count <number_of_peers>
  ```

- **Show node health**:
  ```bash
  dfs health
  ```

### Editing Files

- **Edit a file**:
  ```bash
  dfs edit --key <file_key>
  ```

## Architecture

![Architecture Diagram](https://raw.githubusercontent.com/priyangshupal/documentation-images/main/cas-distributed-file-system/architecture.svg)

The system consists of multiple peers that communicate over TCP. Each peer stores files locally and replicates them across the network for redundancy. The architecture supports:

- **Content-based addressing**: Files are identified by their hash.
- **Distributed storage**: Files are stored across multiple peers.
- **Fault tolerance**: Data is replicated to ensure availability even if some peers fail.

## [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
This project is licensed under the [MIT License](https://opensource.org/license/mit). See the [LICENSE](LICENSE) file for details.
