# Go Package Playground

Welcome to the **Go Package Playground**! This project serves as a playground for experimenting with different Go packages. The goal is to explore various Go packages and concepts in real-world scenarios.

The project is structured with package definitions under the `pkg` folder and implementations under the `apps` folder to demonstrate practical use cases and integrations.

## Project Structure
    .
    ├── apps
    │   ├── consumer            # Consumer service
    │   ├── producer            # Producer service
    ├── pkg                    
    │   ├── rabbitmq            # RabbitMQ package
    └── go.work
    └── docker-compose.yml
    └── README.md
    └── LICENSE.md
    └── ...
## Official Package 
go to https://github.com/afaf-tech/go-rabbitmq

## Project Structure
    .
    ├── apps
    │   ├── consumer            # Consumer service
    │   ├── producer            # Producer service
    ├── pkg                    
    │   ├── rabbitmq            # rabbitmq package
    └── go.work
    └── docker-compose.yml
    └── README.md
    └── LICENSE.md
    └── ...

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
3. Open new terminal Navigate to the producer directory
4. Run Producer service:
```bash
    go run main.go
```
read more [PRODUCER.md](./apps/producer/README.md)


## Backlog
- Proper Logging
- Producer function
- Improve the Consumer connection and channel abstraction


## License
This project is licensed under the MIT License - see the LICENSE.md file for details.
