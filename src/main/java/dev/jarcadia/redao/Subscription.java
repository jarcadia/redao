package dev.jarcadia.redao;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import dev.jarcadia.redao.exception.RedisCommandoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;

public class Subscription implements Closeable {
	
    private final Logger logger = LoggerFactory.getLogger(Subscription.class);
    
    private final StatefulRedisPubSubConnection<String, String> pubsubConnection;
    private final Listener listener;
    
    protected Subscription(StatefulRedisPubSubConnection<String, String> pubsubConnection, ValueFormatter formatter, BiConsumer<String, String> consumer) {
        this.pubsubConnection = pubsubConnection;
        RedisPubSubAsyncCommands<String, String> async = pubsubConnection.async();
        this.listener = new Listener(async, consumer);
        this.pubsubConnection.addListener(listener);
    }

    protected Subscription(StatefulRedisPubSubConnection<String, String> pubsubConnection, ValueFormatter formatter, BiConsumer<String, String> consumer, String channel) {
    	this(pubsubConnection, formatter, consumer);
    	this.subscribe(channel);
    }
    
    public void subscribe(String channel) {
    	this.listener.subscribe(channel);
    }
    
    public void subscribeOnce(String channel) {
    	this.listener.subscribeOnce(channel);
    }
    
    public void unsubscribe(String channel) {
    	this.listener.unsubscribe(channel);
    }
    
    @Override
    public void close() {
    	this.listener.unsubscribeAll();
        this.pubsubConnection.close();
    }
    
    private class Listener implements RedisPubSubListener<String, String> {

    	private final RedisPubSubAsyncCommands<String, String> async;
    	private final BiConsumer<String, String> consumer;
    	private final Set<String> subscriptions;
    	private final Set<String> onces;

    	public Listener(RedisPubSubAsyncCommands<String, String> async, BiConsumer<String, String> consumer) {
    		this.async = async;
    		this.consumer = consumer;
    		this.subscriptions = ConcurrentHashMap.newKeySet();
    		this.onces = ConcurrentHashMap.newKeySet();
		}
    	
    	public void subscribe(String channel) {
            this.subscriptions.add(channel);
            try {
				this.async.subscribe(channel).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RedisCommandoException("Unable to subscribe to " + channel);
			}
    	}
    	
    	public void subscribeOnce(String channel) {
            this.onces.add(channel);
            this.subscribe(channel);
    	}
    	
    	public void unsubscribe(String channel) {
    		Future<Void> future = this.async.unsubscribe(channel);
    		try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RedisCommandoException("Unable to unsubscribe from " + channel);
			}
    	}
    	
    	public void unsubscribeAll() {
    		String[] channels = subscriptions.stream().collect(Collectors.toList()).toArray(new String[0]);
    		Future<Void> future = this.async.unsubscribe(channels);
    		try {
                future.get();
			} catch (InterruptedException | ExecutionException e) {
				logger.warn("Unexpected exception while unsubscribing from " + channels.toString());
			}
    	}

        @Override
        public void message(String channel, String message) {
            consumer.accept(channel, message);
            if (onces.remove(channel)) {
            	this.async.unsubscribe(channel);
            }
        }

        @Override
        public void subscribed(String channel, long count) {
        	logger.debug("Subscribed to {} ({} channels subscribed)", channel, count);
        }

        @Override
        public void unsubscribed(String channel, long count) {
        	this.subscriptions.remove(channel);
        	this.onces.remove(channel); // In case unsubscribe was called before one-time message was rcvd
        	logger.debug("Active subscriptions: {} {}", subscriptions.size(), subscriptions);
        }

        @Override
        public void psubscribed(String pattern, long count) { }

        @Override
        public void punsubscribed(String pattern, long count) { }

        @Override
        public void message(String pattern, String channel, String message) { }

    };
}
