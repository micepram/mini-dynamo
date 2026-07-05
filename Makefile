# mini-dynamo — common flows. JDK 21 is pinned for Gradle invocations.
# Prefer the Homebrew openjdk@21 (keg-only, so `java_home -v 21` can't see it and would
# silently fall back to a newer JDK); otherwise let java_home resolve a registered 21.
JDK21_BREW := /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
export JAVA_HOME := $(shell [ -d $(JDK21_BREW) ] && echo $(JDK21_BREW) || /usr/libexec/java_home -v 21)

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

# Sloppy-quorum + hinted-handoff demo (kills and recovers a node)
demo-resilience:
	./scripts/resilience-demo.sh
