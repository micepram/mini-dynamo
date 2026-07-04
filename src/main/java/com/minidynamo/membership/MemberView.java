package com.minidynamo.membership;

import com.minidynamo.ring.Node;

/**
 * The gossiped view of a member (spec §6): identity plus the two values merged by the protocol.
 * Local-only fields (state, last-heard time) are not gossiped — each node derives state itself.
 */
public record MemberView(Node node, long heartbeat, long incarnation) {}
