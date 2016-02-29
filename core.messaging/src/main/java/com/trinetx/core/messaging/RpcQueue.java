package com.trinetx.core.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    private static Logger logger = LoggerFactory.getLogger(RpcQueue.class);;
    private static final String TERM_SERVER_QUEUE_NAME = "termServerQueue";
      
    private static Map<String, RpcQueue> queues = new HashMap<String, RpcQueue>();
    private static String queueName = "Unnamed_Queue";
    private static ConnectionFactory connectionFactory;
    private static Connection sendConnection = null;
	private static Connection receiveConnection = null;
    private static Channel sendChannel;
	private static Channel receiveChannel = null;
    private static QueueingConsumer requestConsumer;
	private static QueueingConsumer responseConsumer;

	// executor pool for handling received messages
	private static ExecutorService listener = Executors.newSingleThreadExecutor();
	private static ReceiveWorker worker ;

	public static final RpcQueue getTermServerQueue() {
		return getQueue(TERM_SERVER_QUEUE_NAME);
	}
		
    private static ShutdownListener s_sendShutdownListener = new ShutdownListener() {
        @Override
        public void shutdownCompleted(final ShutdownSignalException cause) {
            logger.info("====== RpcQueue: rabbitMQ shutdown: " + cause.getMessage());
    		asyncSendReconnect(5);
        }
    };

    private static ShutdownListener s_receiveShutdownListener = new ShutdownListener() {
        @Override
        public void shutdownCompleted(final ShutdownSignalException cause) {
            logger.info("====== RpcQueue: rabbitMQ shutdown: " + cause.getMessage());
    		asyncReceiveReconnect(5);
        }
    };

    private RpcQueue(String name) {
    	queueName = name;
	}

	public static void init() {
        RpcQueueSetting.init(Hostname.getMyName());

        // instantiate connection factory
        connectionFactory = new ConnectionFactory();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setConnectionTimeout(30000);

        // instantiate queues in RpcQueueSetting
        RpcQueueSetting.getQueues().forEach((k,v)->{
        	queues.put(k, new RpcQueue(k));
        });
    }

    public byte[] send(final byte[] data) throws Exception {
    	sendConnect();
    	return doSend(data);
    }

    public void receive(Function<String,String> f) throws Exception {
    	receiveConnect();
    	doReceive(f);
    }

    synchronized static void shutdown() throws IOException {
    	
    	shutdownSendConnection();
    	shutdownReceiveConnection();
    	shutdownListener();
    	
        logger.info("====== RpcQueue: shutdown");
    }

    synchronized static void shutdownSendConnection() {
    	if (sendConnection != null) {
    		try {
    			sendConnection.removeShutdownListener(s_sendShutdownListener);
    			sendChannel.close();
				sendConnection.close();
			} catch (IOException e) {
				// ignore IOException as connection will be recreated
			}
        	sendConnection = null;
    	}
	}

    synchronized static void shutdownReceiveConnection() {
    	if (receiveConnection != null) {
    		try {
    			receiveConnection.removeShutdownListener(s_receiveShutdownListener);;
    			receiveChannel.close();
				receiveConnection.close();
			} catch (IOException e) {
				// ignore IOException as connection will be recreated
			}
    		receiveConnection = null;
    	}
	}

    synchronized static void shutdownListener() {
    	if (! listener.isShutdown()) {
	        listener.shutdown();
    	}
    }
    
    synchronized static void sendConnect() {
        if (sendConnection != null && sendConnection.isOpen()) {
            // already connected
            return;
        }

        logger.info("====== RpcQueue: sender connecting");
        RpcQueueSetting.apply(connectionFactory);

        try {
        	sendConnection = connectionFactory.newConnection();
            sendChannel = sendConnection.createChannel();

            // create non-durable response queue and set as consumer
            sendChannel.queueDeclare(getResponseQueueName(), true, false, false, ImmutableMap.of("x-ha-policy", "all")); 
            responseConsumer = new QueueingConsumer(sendChannel);
            sendChannel.basicConsume(getResponseQueueName(), true, responseConsumer);
            logger.info("====== RpcQueue: sender connected");
            sendConnection.addShutdownListener(s_sendShutdownListener);
        }
        catch (final Exception e) {
            logger.error("sendConnect: {}", e.getMessage());
            asyncSendReconnect(5);
        }
    }

    synchronized static void receiveConnect() {
        if (receiveConnection != null && receiveConnection.isOpen()) {
            // already connected
            return;
        }

        logger.info("====== RpcQueue: receiver connecting");
        RpcQueueSetting.apply(connectionFactory);

        try {
        	receiveConnection = connectionFactory.newConnection();
        	receiveChannel = receiveConnection.createChannel();

            // create non-durable request queue and set as consumer
        	receiveChannel.queueDeclare(getRequestQueueName(), true, false, false, ImmutableMap.of("x-ha-policy", "all")); 
        	receiveChannel.basicQos(1);
            requestConsumer = new QueueingConsumer(receiveChannel);
            receiveChannel.basicConsume(getRequestQueueName(), false, requestConsumer);
            logger.info("====== RpcQueue: receiver connected");
            receiveConnection.addShutdownListener(s_receiveShutdownListener);
        }
        catch (final Exception e) {
            logger.error("receiveConnect: {}", e.getMessage());
            asyncReceiveReconnect(5);
        }
    }

	static RpcQueue getQueue(String queue) {
		return queues.get(queue);
	}
	
    static String getRequestQueueName() {
    	return RpcQueueSetting.getQueue(queueName) + "_request_queue";
    }
    
    static String getResponseQueueName() {
    	return RpcQueueSetting.getQueue(queueName) + "_response_queue";
    }
    
    private static byte[] doSend(byte[] data) throws Exception {
	    String corrId = java.util.UUID.randomUUID().toString();
	
	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(getResponseQueueName())
	                                .build();
	
	    sendChannel.basicPublish("", getRequestQueueName(), props, data);
	
	    while (true) {
	        Delivery delivery = responseConsumer.nextDelivery();
	        if (delivery.getProperties().getCorrelationId().equals(corrId)) {
	            return delivery.getBody();
	        }
	    }
    }
    
    private static void doReceive(Function<String,String> f)  {
    	worker = new ReceiveWorker(requestConsumer, receiveChannel, f);
    	listener.execute(worker);
    }

    private static void restartWorker() {
    	// shutdown worker thread, set new consumer and channel values, then set it running again in a new thread
        worker.shutdown();
        worker.setConsumer(requestConsumer);
        worker.setChannel(receiveChannel);
        worker.setRunning();
    	listener.execute(worker);
        logger.info("====== RpcQueue: worker thread restarted");
    }
    
    private static void asyncSendReconnect(final int delay) {
    	Scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                	shutdownSendConnection();
                    sendConnect();
                }
                catch (final Exception e) {
                    logger.error("asyncSendConnect: {}", e.getMessage());
                    asyncSendReconnect(5);
                }
            }
        }, delay, TimeUnit.SECONDS);
    }
    
    private static void asyncReceiveReconnect(final int delay) {
    	Scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                	shutdownReceiveConnection();
                    receiveConnect();
                    restartWorker();
                }
                catch (final Exception e) {
                    logger.error("asyncReceiveReconnect: {}", e.getMessage());
                    asyncReceiveReconnect(5);
                }
            }
        }, delay, TimeUnit.SECONDS);
    }
}
