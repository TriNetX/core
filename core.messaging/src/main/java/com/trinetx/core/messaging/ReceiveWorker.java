package com.trinetx.core.messaging;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class ReceiveWorker implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(RpcQueue.class);
    
    private static final int DEFAULT_WORKER_THREADS = 50;
	private static ExecutorService workers;

	private Function<String,String> func;
	private Channel channel;
	private QueueingConsumer consumer;
	
	private volatile boolean shutdown = false;
	
	ReceiveWorker(QueueingConsumer consumer, Channel channel, Function<String,String> func) {
    	this.consumer = consumer;
		this.channel = channel;
		this.func = func;

	    // executor pool for handling received messages
		workers = Executors.newFixedThreadPool(DEFAULT_WORKER_THREADS);
	}
	
	void setConsumer(QueueingConsumer consumer) {
		this.consumer = consumer;
	}
	
	void setChannel(Channel channel) {
		this.channel = channel;
	}

	void shutdown() {
		shutdown = true;
	}
	
	Function<String,String> getFunction() {
		return func;
	}
	
	@Override
    public void run() {
    	while (!shutdown) {
    		try {
	    	    QueueingConsumer.Delivery delivery = consumer.nextDelivery();
	    	    // received a new delivery, dispatch to receivers to process
	    	    workers.submit(new Runnable() {
					@Override
					public void run() {
		    	    	BasicProperties props = delivery.getProperties();
			    	    BasicProperties replyProps = new BasicProperties
			    	                                     .Builder()
			    	                                     .correlationId(props.getCorrelationId())
			    	                                     .build();
		
			    	    String message = func.apply(new String(delivery.getBody()));
			    	    try {
			    	    	channel.basicPublish("", props.getReplyTo(), replyProps, message.getBytes());
			    	    	channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
						} catch (IOException e) {
				            logger.error("Runnable thread: {}", e.getMessage());
						}
					}
    	    	});
			} catch (ShutdownSignalException e1) {
	            logger.error("ReceiveWorker: shutdown signal received from rabbitMQ");
	            shutdown = true;
			} catch (ConsumerCancelledException e1) {
	            logger.error("ReceiveWorker: queue consumer cancelled");
	            shutdown = true;
			} catch (InterruptedException e1) {
	            logger.error("ReceiveWorker: thread is interrupted");
	            shutdown = true;
			}
    	}
    	logger.debug("ReceiveWorker thread was gracefully shutdown");
    }
}
