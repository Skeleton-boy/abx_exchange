## Prerequisites
- Node.js (version 16.17.0 or higher)
- C++ Compiler (g++)
- Basic development tools

## Setup Instructions

### 1. Navigate to Server Directory
```bash
cd abx_exchange_server
```

### 2. Start the Server
1. Ensure you have Node.js installed
2. Run the server:
```bash
node main.js
```
- The server will start on port 3000
- You should see the message "TCP server started on port 3000"

### 3. Open New Terminal Window

### 4. Navigate to Client Directory
```bash
cd ../abx_exchange_client
```

### 5. Compile the C++ Client
```bash
g++ -std=c++11 client_exchange.cpp -o client_exchange -lws2_32
```
For mac--
 ```bash
g++ -std=c++11 client_exchange.cpp -o client_exchange

```

### 6. Run the Client
```bash
./client_exchange
```

### Expected Output
- The client will connect to the server
- Retrieve stock ticker data
- Generate `output.json` file
- Display processing statistics in the terminal

## Troubleshooting
- Ensure Node.js is version 16.17.0 or higher
- Verify that port 3000 is available
- Check network connectivity
- Confirm C++ compiler is installed

## Notes
- The `output.json` file will be generated in the same directory
- The JSON contains all received market data packets
