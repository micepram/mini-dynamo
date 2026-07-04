package com.minidynamo.failure;

import com.minidynamo.ring.Node;
import com.minidynamo.versioning.Record;

/**
 * A record a substitute is holding on behalf of an unreachable intended owner (spec §7), to be
 * delivered when that owner recovers. {@code storedAtMillis} drives retention expiry.
 */
public record Hint(Node intendedNode, String key, Record record, long storedAtMillis) {}
