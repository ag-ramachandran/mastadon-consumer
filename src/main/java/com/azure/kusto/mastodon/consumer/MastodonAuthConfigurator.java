package com.azure.kusto.mastodon.consumer;

import jakarta.websocket.ClientEndpointConfig;

public class MastodonAuthConfigurator extends ClientEndpointConfig.Configurator {
    static volatile boolean called = false;
    private final String mastodonApiKey;

    public MastodonAuthConfigurator() {
        called = true;
        this.mastodonApiKey = System.getenv("MASTODON_API_KEY");
    }

    @Override
    public void beforeRequest(java.util.Map<String, java.util.List<String>> headers) {
        headers.put("Authorization", java.util.Collections.singletonList("Bearer " + mastodonApiKey));
    }

}
