package com.minidynamo.membership;

import com.minidynamo.ring.Node;
import java.util.List;

/**
 * Push-pull gossip exchange with a peer (spec §6): send our views, receive theirs. Behind an
 * interface so the HTTP implementation can be swapped and so the gossip round is unit-testable.
 */
public interface GossipTransport {

    List<MemberView> exchange(Node peer, List<MemberView> views);
}
