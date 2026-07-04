# mini-dynamo — common flows. JDK 21 is pinned for Gradle invocations.
export JAVA_HOME := $(shell /usr/libexec/java_home -v 21 2>/dev/null || echo /opt/homebrew/opt/openjdk@21)

.PHONY: build test up down kill-node demo

build:
	./gradlew build

test:
	./gradlew test

up:
	docker compose up --build -d

down:
	docker compose down

# Kill one node for resilience demos, e.g. make kill-node NODE=node2
kill-node:
	docker compose stop $(NODE)

demo:
	./scripts/demo.sh
