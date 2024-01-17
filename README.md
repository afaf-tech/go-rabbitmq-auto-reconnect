# Go RabbitMQ Auto-Reconnect Project

This project demonstrates a simple implementation of a RabbitMQ consumer and producer in Go with automatic reconnection capabilities.

## Project Structure

project-root/
│
├── apps/
│ ├── consumer/
│ ├── producer/
├── pkg/
│ ├── rabbitmq/
├── docker-compose.yml
├── go.work


## Requirements
- go 1.21.5
- docker

### Installation

1. Clone the repository:
```bash
   git clone 
```
2. Navigate to the project directory
```bash
    cd go-rabbitmq-auto-reconnect
```
3. Install dependencies:
```bash
    go install ./...
```
4. Run RabbitMQ server
```bash
    go install ./...
```

### Running
0. Run rabbitmq server
```bash
docker-compose up -d
```
1. Navigate to the consumer directory:
2. Run consumer service
```bash
    go run main.go
```
3. Open new terminal & Run Producer service:
```bash
    go run main.go
```
read more [PRODUCER.md](./apps/producer/README.md)


## Backlog
- Proper Logging
- Producer function
- Improve the Consumer connection and channel mechanism


## License
This project is licensed under the MIT License - see the LICENSE.md file for details.