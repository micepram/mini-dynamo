package com.minidynamo.api;

import com.minidynamo.membership.MemberView;
import com.minidynamo.membership.MembershipTable;
import java.util.List;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Internal gossip endpoint (spec §6): merge the sender's views, reply with ours (push-pull). */
@RestController
@RequestMapping("/internal/gossip")
public class GossipController {

    private final MembershipTable table;

    public GossipController(MembershipTable table) {
        this.table = table;
    }

    @PostMapping
    public List<MemberView> exchange(@RequestBody List<MemberView> incoming) {
        incoming.forEach(table::merge);
        return table.views();
    }
}
