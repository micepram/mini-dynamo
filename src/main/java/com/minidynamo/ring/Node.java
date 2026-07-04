package com.minidynamo.ring;

/**
 * A physical node's routable identity (spec §4). {@code host} is the node's hostname —
 * by convention its {@code node-id} (the Docker service name / network alias), so a
 * node's advertised address is {@code node-id:port}.
 */
public record Node(String host, int port) {

    public String address() {
        return host + ":" + port;
    }

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }

    /** Parse a {@code host:port} entry (as used in {@code SEEDS}). */
    public static Node parse(String hostPort) {
        int i = hostPort.lastIndexOf(':');
        if (i <= 0 || i == hostPort.length() - 1) {
            throw new IllegalArgumentException("Invalid host:port: " + hostPort);
        }
        return new Node(hostPort.substring(0, i).trim(), Integer.parseInt(hostPort.substring(i + 1).trim()));
    }
}
