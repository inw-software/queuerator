# Queuerator

Quick and lightweight service designed to curate your data queues.
If at any point during the project's lifespan you decide to give it a try, please open an issue if you run into any problems connecting to your data queues or processing any of your messages.

## Getting started

In this repo, I am including a sample configuration and a script to spin up both a RabbitMQ instance for AMQP messaging, and a mosquitto MQTT broker using Docker.

### Prerequisites

This project is written entirely in Go version 1.24.1 and is untested with any other versions.
You can install Go at [go.dev](go.dev/dl).

If you want to use the sample MQTT and AMQP brokers, you will need to install the [Docker Engine CLI](https://docs.docker.com/engine/install/)

### Installation

1. Clone the repo
  ```sh
  git clone https://github.com/inw-software/queuerator
  ```

2. Run the script located at `scripts/broker.sh`
  ```sh
  ./scripts/broker.sh
  ```

2. Log into the the RabbitMQ instance running at `http://localhost:15672` the default user/password combination is `guest`/`guest`.

3. Create a queue to subscribe to. The sample config is pointed to a queue named `test_queue`

## Usage

Once you have your brokers running, you can run the project. **Note**: a configuration file path must be passed to the `--config` flag.
There is a [sample config](https://github.com/inw-software/queuerator/blob/d086f6db918bb1e2b3aab2fe076ba1791108aa75/sample-config.json) included with this repository.

1. Run the project using `go`
  ```sh
  go run cmd/queuerator/main.go --config ./sample-config.json
  ```

2. Using the RabbitMQ web UI, send any message to `test_queue` and watch your terminal output for results.
The current criteria model only operates on JSON messages.

3. Using the Docker CLI, you can send commands to the mosquitto instance that will send a message to your subscribed topic(s).
The sample config is subscribed to the `test/topic` topic.
  ```sh
  docker exec -it mosquitto \
  mosquitto_pub -h localhost -t test/topic -m '{"sensor":42}'
  ```

## Roadmap

This project is still in its earliest phases. I would like to implement the following (in no particular order):
- [x] Add basic AMQP support
- [x] Add basic MQTT support
- [ ] Add basic Kafka Streams support
- [ ] Expand existing data feed support to be much more robust
- [ ] Build a comprehensive logging scheme
- [ ] Implement outbound message queues for propagating messages that satify the configured criteria.
- [ ] Implement LLM prompt-based criteria
- [ ] Design a deployable Dockerfile
- [ ] Add CICD pipeline to automate deployments
- [ ] Formalize JSON schema for configuration files

## Contribution
I am not seeking external contributions at this time. However, anyone may open a pull request that I will happily review and/or merge.


