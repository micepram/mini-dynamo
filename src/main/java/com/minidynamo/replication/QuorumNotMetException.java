package com.minidynamo.replication;

/** Raised when fewer than the required W acks / R responses can be collected (spec §5). */
public class QuorumNotMetException extends RuntimeException {

    public QuorumNotMetException(int required, int available, String detail) {
        super("quorum not met: required " + required + ", available " + available + " (" + detail + ")");
    }
}
