package com.minidynamo.membership;

import com.minidynamo.ring.Node;

/**
 * A member's full local record (spec §6). {@code lastUpdatedMillis} is when this node last heard a
 * fresher heartbeat; the failure detector derives {@link MemberState} from its age. Local-only.
 */
public record MemberInfo(
        Node node, MemberState state, long heartbeat, long incarnation, long lastUpdatedMillis) {

    MemberInfo withState(MemberState newState) {
        return new MemberInfo(node, newState, heartbeat, incarnation, lastUpdatedMillis);
    }
}
