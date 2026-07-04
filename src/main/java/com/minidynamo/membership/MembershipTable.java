package com.minidynamo.membership;

import com.minidynamo.ring.Node;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gossip-maintained membership table (spec §6): {@code nodeId → {state, heartbeat, incarnation}}.
 * Merge rule: an incoming view wins on higher incarnation, then higher heartbeat. The failure
 * detector ({@link #sweep}) ages entries ALIVE → SUSPECT → DEAD by how long since a fresher
 * heartbeat was heard. Wall-clock time is used only for failure timing (never for record LWW).
 * Thread-safe: entries live in a {@link ConcurrentHashMap} keyed by node address.
 */
public class MembershipTable {

    private static final Logger log = LoggerFactory.getLogger(MembershipTable.class);

    private final Node self;
    private final long failureTimeoutMs;
    private final long selfIncarnation;
    private final LongSupplier nowMillis;

    private final ConcurrentHashMap<String, MemberInfo> members = new ConcurrentHashMap<>();
    private final AtomicLong selfHeartbeat = new AtomicLong(0);
    private final AtomicLong nodeSetVersion = new AtomicLong(0);

    public MembershipTable(
            Node self, Collection<Node> seeds, long failureTimeoutMs, long selfIncarnation, LongSupplier nowMillis) {
        this.self = self;
        this.failureTimeoutMs = failureTimeoutMs;
        this.selfIncarnation = selfIncarnation;
        this.nowMillis = nowMillis;

        long now = nowMillis.getAsLong();
        members.put(self.address(), new MemberInfo(self, MemberState.ALIVE, 0, selfIncarnation, now));
        for (Node seed : seeds) {
            if (!seed.equals(self)) {
                members.putIfAbsent(seed.address(), new MemberInfo(seed, MemberState.ALIVE, 0, 0, now));
            }
        }
    }

    /** Advance and publish this node's own heartbeat (called each gossip round). */
    public void bumpSelfHeartbeat() {
        long hb = selfHeartbeat.incrementAndGet();
        members.put(self.address(), new MemberInfo(self, MemberState.ALIVE, hb, selfIncarnation, nowMillis.getAsLong()));
    }

    /** Merge one gossiped view. Returns true if it introduced a previously unknown node. */
    public boolean merge(MemberView view) {
        if (view.node().equals(self)) {
            return false; // peers never dictate our own entry
        }
        long now = nowMillis.getAsLong();
        MemberInfo prev = members.get(view.node().address());
        if (prev == null) {
            members.put(view.node().address(), new MemberInfo(view.node(), MemberState.ALIVE, view.heartbeat(), view.incarnation(), now));
            nodeSetVersion.incrementAndGet();
            log.info("member joined {}", view.node().address());
            return true;
        }
        boolean fresher = view.incarnation() > prev.incarnation()
                || (view.incarnation() == prev.incarnation() && view.heartbeat() > prev.heartbeat());
        if (fresher) {
            // A fresher heartbeat revives the member to ALIVE and resets its age.
            members.put(view.node().address(), new MemberInfo(view.node(), MemberState.ALIVE, view.heartbeat(), view.incarnation(), now));
            if (prev.state() != MemberState.ALIVE) {
                log.info("member {} {} -> ALIVE", view.node().address(), prev.state());
            }
        }
        return false;
    }

    /** Age every peer entry: ALIVE within one timeout, SUSPECT within two, DEAD beyond. */
    public void sweep() {
        long now = nowMillis.getAsLong();
        for (MemberInfo member : members.values()) {
            if (member.node().equals(self)) {
                continue;
            }
            long age = now - member.lastUpdatedMillis();
            MemberState next = age > 2 * failureTimeoutMs
                    ? MemberState.DEAD
                    : age > failureTimeoutMs ? MemberState.SUSPECT : MemberState.ALIVE;
            if (next != member.state()) {
                members.put(member.node().address(), member.withState(next));
                log.info("member {} {} -> {}", member.node().address(), member.state(), next);
            }
        }
    }

    /** Views to gossip to a peer (self carries its live heartbeat). */
    public List<MemberView> views() {
        return members.values().stream()
                .map(m -> new MemberView(
                        m.node(), m.node().equals(self) ? selfHeartbeat.get() : m.heartbeat(), m.incarnation()))
                .toList();
    }

    public Collection<MemberInfo> snapshot() {
        return List.copyOf(members.values());
    }

    /** Available for routing: self, or a peer not detected DEAD. */
    public boolean isAvailable(Node node) {
        if (node.equals(self)) {
            return true;
        }
        MemberInfo member = members.get(node.address());
        return member != null && member.state() != MemberState.DEAD;
    }

    public MemberState stateOf(Node node) {
        if (node.equals(self)) {
            return MemberState.ALIVE;
        }
        MemberInfo member = members.get(node.address());
        return member == null ? MemberState.DEAD : member.state();
    }

    public Set<Node> nodes() {
        return members.values().stream().map(MemberInfo::node).collect(Collectors.toSet());
    }

    public long nodeSetVersion() {
        return nodeSetVersion.get();
    }

    public Node self() {
        return self;
    }
}
