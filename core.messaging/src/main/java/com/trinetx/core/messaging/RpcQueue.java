package com.trinetx.core.messaging;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Created by Wai Cheng on 2/24/16.
 */
public class RpcQueue {
	
    private ShutdownListener s_ShutdownListener = new ShutdownListener() {
            @Override
            public void shutdownCompleted(final ShutdownSignalException cause) {
                s_Logger.info("====== RpcQueue: shutdown: " + cause.getMessage());
                listener.shutdown();
                s_sendConnection = null;
                s_receiveChannel = null;
            }
        };

    private static Logger s_Logger = null;

    private String s_QueueName;
    private ConnectionFactory s_ConnectionFactory;
    private Connection s_sendConnection, s_receiveConnection = null;
    private Channel s_sendChannel, s_receiveChannel = null;
    private QueueingConsumer s_RequestConsumer, s_ResponseConsumer;

	private ExecutorService listener = Executors.newSingleThreadExecutor();
	private ExecutorService processors;

    public RpcQueue(String queueName) {
    	this(queueName, 20);
	}

    public RpcQueue(String queueName, int count) {
    	s_QueueName = queueName;
    	processors = Executors.newFixedThreadPool(count);
    	init();
	}

	private void init() {
        if (s_Logger == null) {
            s_Logger = LoggerFactory.getLogger(RpcQueue.class);
        }
        RpcQueueSetting.init(Hostname.getMyName());
    }

    public byte[] send(final byte[] data) throws Exception {
    	sendConnect();
    	return doSend(data);
    }

    public void receive(Function<String,String> f) throws Exception {
    	receiveConnect();
    	doReceive(f);
    }

    synchronized void sendConnect() throws IOException {
        if (s_sendConnection != null) {
            // already connected
            return;
        }

        s_Logger.info("====== RpcQueue: send connecting");
        s_ConnectionFactory = new ConnectionFactory();
        RpcQueueSetting.apply(s_ConnectionFactory);
        s_ConnectionFactory.setConnectionTimeout(2500);

        try {
        	s_sendConnection = s_ConnectionFactory.newConnection();
            s_sendChannel = s_sendConnection.createChannel();

            // create non-durable response queue and set as consumer
            s_sendChannel.queueDeclare(getResponseQueueName(), false, false, false, ImmutableMap.of("x-ha-policy", "all")); 
            s_ResponseConsumer = new QueueingConsumer(s_sendChannel);
            s_sendChannel.basicConsume(getResponseQueueName(), true, s_ResponseConsumer);
            s_Logger.info("====== RpcQueue: send connected");
        }
        catch (final Exception e) {
            s_Logger.error("Connect: {}", e);
            s_sendConnection = null;
            throw e;
        }
        s_sendConnection.addShutdownListener(s_ShutdownListener);
    }

    synchronized void receiveConnect() throws IOException {
        if (s_receiveConnection != null) {
            // already connected
            return;
        }

        s_Logger.info("====== RpcQueue: receiver connecting");
        s_ConnectionFactory = new ConnectionFactory();
        RpcQueueSetting.apply(s_ConnectionFactory);
        s_ConnectionFactory.setConnectionTimeout(2500);

        try {
        	s_receiveConnection = s_ConnectionFactory.newConnection();
        	s_receiveChannel = s_receiveConnection.createChannel();

            // create non-durable request queue and set as consumer
        	s_receiveChannel.queueDeclare(getRequestQueueName(), false, false, false, ImmutableMap.of("x-ha-policy", "all")); 
        	s_receiveChannel.basicQos(1);
            s_RequestConsumer = new QueueingConsumer(s_receiveChannel);
            s_receiveChannel.basicConsume(getRequestQueueName(), false, s_RequestConsumer);
            s_Logger.info("====== RpcQueue: receiver connected");
        }
        catch (final Exception e) {
            s_Logger.error("Connect: {}", e);
            s_receiveConnection = null;
            throw e;
        }
        s_receiveConnection.addShutdownListener(s_ShutdownListener);
    }

    private String getRequestQueueName() {
    	return RpcQueueSetting.getQueue(s_QueueName) + "_request_queue";
    }
    
    private String getResponseQueueName() {
    	return RpcQueueSetting.getQueue(s_QueueName) + "_response_queue";
    }
    
    private byte[] doSend(byte[] data) throws Exception {
	    String corrId = java.util.UUID.randomUUID().toString();
	
	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(getResponseQueueName())
	                                .build();
	
	    s_sendChannel.basicPublish("", getRequestQueueName(), props, data);
	
	    while (true) {
	        Delivery delivery = s_ResponseConsumer.nextDelivery();
	        if (delivery.getProperties().getCorrelationId().equals(corrId)) {
	            return delivery.getBody();
	        }
	    }
    }
    
    private void doReceive(Function<String,String> f) {
    	listener.execute(new Runnable() {
    	    public void run() {
    	    	while (true) {
    	    		try {
			    	    QueueingConsumer.Delivery delivery = s_RequestConsumer.nextDelivery();
		    			processors.submit(new Runnable() {
							@Override
							public void run() {
				    	    	BasicProperties props = delivery.getProperties();
					    	    BasicProperties replyProps = new BasicProperties
					    	                                     .Builder()
					    	                                     .correlationId(props.getCorrelationId())
					    	                                     .build();
				
					    	    String message = f.apply(new String(delivery.getBody()));
					    	    try {
					    	    	s_receiveChannel.basicPublish("", props.getReplyTo(), replyProps, message.getBytes());
					    	    	s_receiveChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
		    	    	});
    	    		} catch (Exception e) {
    	    			// TODO:
    	    		}
    	    	}
    	    }
    	});
    }
}
