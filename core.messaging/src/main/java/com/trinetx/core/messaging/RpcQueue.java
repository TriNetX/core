package com.trinetx.core.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.trinetx.utils.Hostname;

/**
 * Created by Wai Cheng on 2/24/16.
 */
public class RpcQueue {
	
    private ShutdownListener s_ShutdownListener = new ShutdownListener() {
            @Override
            public void shutdownCompleted(final ShutdownSignalException cause) {
                s_Logger.info("====== RpcQueue: shutdown: " + cause.getMessage());
                s_BusConnection = null;
            }
        };

    private static Logger s_Logger = null;

    private String s_QueueName;
    private ConnectionFactory s_ConnectionFactory;
    private Connection s_BusConnection = null;
    private Channel s_Channel = null;
    private QueueingConsumer s_RequestConsumer, s_ResponseConsumer;

	private ExecutorService listener = Executors.newSingleThreadExecutor();
	private ExecutorService processors;

    public RpcQueue(String queueName) {
    	this(queueName, 20);
	}

    public RpcQueue(String queueName, int processor) {
    	s_QueueName = queueName;
    	processors = Executors.newFixedThreadPool(processor);
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
    	return call(data);
    }

    public void listen(Function<String,String> f) throws Exception {
    	receiveConnect();
    	receive(f);
    }

    private synchronized void sendConnect() throws IOException {
        if (s_BusConnection != null) {
            // already connected
            return;
        }

        s_Logger.info("====== RpcQueue: send connecting");
        s_ConnectionFactory = new ConnectionFactory();
        RpcQueueSetting.apply(s_ConnectionFactory);
        s_ConnectionFactory.setConnectionTimeout(2500);

        try {
            s_BusConnection = s_ConnectionFactory.newConnection();
            s_Channel = s_BusConnection.createChannel();

            final Map<String, Object> queueHAPolicy = new HashMap<String, Object>();
            queueHAPolicy.put("x-ha-policy", "all");

            // create response queue and set as consumer
            s_Channel.queueDeclare(getResponseQueueName(), false, false, false, queueHAPolicy); 
            s_ResponseConsumer = new QueueingConsumer(s_Channel);
            s_Channel.basicConsume(getResponseQueueName(), true, s_ResponseConsumer);
            s_Logger.info("====== RpcQueue: send connected");
        }
        catch (final Exception e) {
            s_Logger.error("Connect: {}", e);
            s_BusConnection = null;
            throw e;
        }
        s_BusConnection.addShutdownListener(s_ShutdownListener);
    }

    private synchronized void receiveConnect() throws IOException {
        if (s_BusConnection != null) {
            // already connected
            return;
        }

        s_Logger.info("====== RpcQueue: receiver connecting");
        s_ConnectionFactory = new ConnectionFactory();
        RpcQueueSetting.apply(s_ConnectionFactory);
        s_ConnectionFactory.setConnectionTimeout(2500);

        try {
            s_BusConnection = s_ConnectionFactory.newConnection();
            s_Channel = s_BusConnection.createChannel();

            final Map<String, Object> queueHAPolicy = new HashMap<String, Object>();
            queueHAPolicy.put("x-ha-policy", "all");

            // create request queue and set as consumer
            s_Channel.queueDeclare(getRequestQueueName(), false, false, false, queueHAPolicy); 
            s_Channel.basicQos(1);
            s_RequestConsumer = new QueueingConsumer(s_Channel);
            s_Channel.basicConsume(getRequestQueueName(), false, s_RequestConsumer);
            s_Logger.info("====== RpcQueue: receiver connected");
        }
        catch (final Exception e) {
            s_Logger.error("Connect: {}", e);
            s_BusConnection = null;
            throw e;
        }
        s_BusConnection.addShutdownListener(s_ShutdownListener);
    }

    private String getRequestQueueName() {
    	return RpcQueueSetting.getQueue(s_QueueName) + "_request_queue";
    }
    
    private String getResponseQueueName() {
    	return RpcQueueSetting.getQueue(s_QueueName) + "_response_queue";
    }
    
    private byte[] call(byte[] data) throws Exception {
	    String corrId = java.util.UUID.randomUUID().toString();
	
	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(getResponseQueueName())
	                                .build();
	
	    s_Channel.basicPublish("", getRequestQueueName(), props, data);
	
	    while (true) {
	        Delivery delivery = s_ResponseConsumer.nextDelivery();
	        if (delivery.getProperties().getCorrelationId().equals(corrId)) {
	            return delivery.getBody();
	        }
	    }
    }
    
    private void receive(Function<String,String> f) {
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
									s_Channel.basicPublish("", props.getReplyTo(), replyProps, message.getBytes());
						    	    s_Channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
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
    
    public void stopListen() {
    	listener.shutdown();
    }
}
