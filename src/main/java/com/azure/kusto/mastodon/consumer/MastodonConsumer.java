package com.azure.kusto.mastodon.consumer;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.websocket.*;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


@ClientEndpoint(configurator = MastodonAuthConfigurator.class)
public class MastodonConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MastodonConsumer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final BlockingQueue<String> BLOCKING_QUEUE = new LinkedBlockingQueue<>(100);
    private static final String MASTODON_URL = System.getenv("MASTODON_URL");
    private static final String EH_CONNECTION_STRING = System.getenv("EH_CONNECTION_STRING");
    private static final Object waitLock = new Object();

    private static void wait4TerminateSignal() {
        synchronized (waitLock) {
            try {
                waitLock.wait();
            } catch (InterruptedException e) {
                LOG.error("Interrupted exception waiting for termination", e);
            }
        }
    }

    public static void main(String[] args) {
        WebSocketContainer container = null;//
        Session session = null;
        try {
            container = ContainerProvider.getWebSocketContainer();
            session = container.connectToServer(MastodonConsumer.class, URI.create(MASTODON_URL));
            wait4TerminateSignal();
        } catch (Exception e) {
            LOG.error("Error in main ", e);
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception e) {
                    LOG.error("Error closing session ", e);
                }
            }
        }
    }

    @OnMessage
    public void onMessage(String message) {
        // System.out.println("Received msg: " + message);
        if (BLOCKING_QUEUE.remainingCapacity() == 0) {
            System.out.println("**Blocking queue is full, sending to EH**");
            sendToEventHub();
        } else {
            if (!"".equals(message) && !message.contains("delete")) {
                BLOCKING_QUEUE.add(message);
            }
        }
    }
    private void sendToEventHub() {
        try (EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(EH_CONNECTION_STRING)
                .buildProducerClient()) {
            EventDataBatch batch = producer.createBatch();
            int messageCount = 0;
            for (String dequeuedMessage : BLOCKING_QUEUE) {
                MastodonTwoot mastodonTwoot = OBJECT_MAPPER.readValue(dequeuedMessage,MastodonTwoot.class);
                String payload = StringEscapeUtils.unescapeJson(mastodonTwoot.payload);
                EventData event = new EventData(payload.getBytes(StandardCharsets.UTF_8));
                batch.tryAdd(event);
                messageCount++;
            }
            producer.send(batch);
            System.out.println("Completed : Sending batch to EH. Batch size: " + messageCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            BLOCKING_QUEUE.clear();
        }
    }
    public static class MastodonTwoot {
        @JsonIgnore
        private List<String> stream;
        private String event;
        private String payload;

        public List<String> getStream() {
            return stream;
        }

        public void setStream(List<String> stream) {
            this.stream = stream;
        }

        public String getEvent() {
            return event;
        }

        public void setEvent(String event) {
            this.event = event;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }
    }
}