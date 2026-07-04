package com.minidynamo.membership;

import com.minidynamo.ring.Node;
import java.util.List;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** HTTP/JSON gossip exchange over {@link RestClient} (spec §3.2, §6). */
@Component
public class RestClientGossipTransport implements GossipTransport {

    private static final ParameterizedTypeReference<List<MemberView>> VIEW_LIST =
            new ParameterizedTypeReference<>() {};

    private final RestClient client;

    public RestClientGossipTransport(RestClient internalRestClient) {
        this.client = internalRestClient;
    }

    @Override
    public List<MemberView> exchange(Node peer, List<MemberView> views) {
        List<MemberView> response = client.post()
                .uri(peer.baseUrl() + "/internal/gossip")
                .contentType(MediaType.APPLICATION_JSON)
                .body(views)
                .retrieve()
                .body(VIEW_LIST);
        return response == null ? List.of() : response;
    }
}
