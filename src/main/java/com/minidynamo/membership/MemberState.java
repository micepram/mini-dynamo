package com.minidynamo.membership;

/** Health state of a member in the membership table (spec §6). */
public enum MemberState {
    ALIVE,
    SUSPECT,
    DEAD
}
