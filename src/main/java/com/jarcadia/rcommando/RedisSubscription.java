package com.jarcadia.rcommando;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

public class RedisSubscription {
    
    private final StatefulRedisPubSubConnection<String, String> pubsubConnection;
    private final RedisPubSubCommands<String, String> sync;
    private final String channel;
    private final CompletableFuture<Void> unsubscribeFuture;
    
    protected RedisSubscription(StatefulRedisPubSubConnection<String, String> pubsubConnection, RedisValueFormatter formatter, String channel, Consumer<String> consumer) {
        this.channel = channel;
        this.unsubscribeFuture = new CompletableFuture<>();
        this.pubsubConnection = pubsubConnection;
        this.sync = pubsubConnection.sync();

        RedisPubSubListener<String, String> listener = new RedisPubSubListener<String, String>() {

            @Override
            public void message(String channel, String message) {
                consumer.accept(message);
            }

            @Override
            public void message(String pattern, String channel, String message) { }

            @Override
            public void subscribed(String channel, long count) {
            }

            @Override
            public void psubscribed(String pattern, long count) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void unsubscribed(String channel, long count) {
                unsubscribeFuture.complete(null);
            }

            @Override
            public void punsubscribed(String pattern, long count) {
                // TODO Auto-generated method stub
                
        }};
        pubsubConnection.addListener(listener);
        sync.subscribe(channel);
    }
    
    public void close() throws InterruptedException, ExecutionException {
        sync.unsubscribe(channel);
        unsubscribeFuture.get();
        pubsubConnection.close();
    }
}
