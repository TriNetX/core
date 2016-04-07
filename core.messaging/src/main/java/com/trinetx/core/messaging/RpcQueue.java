package com.trinetx.core.messaging;

import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * An implementation of rpc messaging backed by durable, non-exclusive and non-auto-delete AMQP queues.
 * Queue messages are sent from the senders and placed into the named AMQP queues, with receivers listening 
 * for queued messages executed in threads from a thread pool to ensure high message processing throughput.
 * 
 * Just like any rpc calls, the sender is blocked until a response is returned or the message times out by the
 * underlying AMQP queue.  No timeout monitor or watchdog is provided in this class so clients or senders
 * should implement their own timeout mechanism.
 * 
 * The RpcQueue implementation would handle both network and AMQP interruptions gracefully without losing queued messages.
 * Therefore the receivers would not need any explicit exception handling.  However, an Exception would be thrown
 * upon sending a message durng service interruption and needs to have handled.
 * 
 * To use RpcQueue, a "rpcQueueSetting" section must be specified in a config yml file similar to the MsgBusSetting
 * with the following information:
 * 
 *      rpcQueueSetting:
 *          Host: 10.88.0.4
 *          # use the default AMQP/RabbitMQ port
 *          Port: 0
 *          User: <user>
 *          Password: <password>
 *          Queues:
 *              <queue_var_name>: <AMQP_rpc_queue_name>     #e: termServerQueue: "{host}_local_term_server"
 * 
 * 
 * The RpcQueue must be initialized with init() on the receiver prior to the receive() call, which takes a 
 * Function block argument used for processing the received messages, otherwise the RpcQueue returned will be null.
 * 
 * For example:
 * 
 *           RpcQueue.init();
 *           RpcQueue termServerQueue = RpcQueueHelper.getTermServerQueue();
 *           termServerQueue.receive(dispatcher.getServiceFunction());
 *
 * To send a rpc message, the sender would get the respective queue handle using RpcQueueHelper and invoke the send() method.
 * The send() method takes a byte[] input and returns another byte[].  It is strongly suggested that additional service
 * layers and models are added on top od RpcQueue for domain specific usage of RpcQueue.  An example of the sender:
 * 
 *          Map<String, String> queryParams = new HashMap<String,String>;
 *          queryParams.put(MessageTypes.TERM_SERVER.COMMAND, MessageTypes.TERM_SERVER.CATEGORY_AND_NAME_RESOURCE);
 *          queryParams.put(MessageTypes.TERM_SERVER.TSParam.CATEGORY_AND_NAME_TERMS, new ObjectMapper().writeValueAsString(terms));
 *          byte[] msgPayload = new ObjectMapper().writeValueAsString(queryParams).getBytes();
 *          byte[] res = RpcQueueHelper.getTermServerQueue().send(msgPayload);
 *          result = new String(res, "UTF-8");
 *
 * Created by Wai Cheng on 2/24/16.
 * 
 */
public class RpcQueue {
    private static Logger logger = LoggerFactory.getLogger(RpcQueue.class);;
      
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

    /**
     * Method for initializing the RpcQueue internals, using RpcQueueSetting.
     */
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

    /**
     * Send a message in byte[] to the RpcQueue, returns the result in anothe byte[].
     * 
     * @param data - byte[] of the message to be sent
     * @return - byte[] of the result
     * @throws Exception
     */
    public byte[] send(final byte[] data) throws Exception {
        sendConnect();
        return doSend(data);
    }

    /**
     * Called by the message receiver and specify how the received message are processed.
     * 
     * @param f - function to be used by receiver in processing messages
     */
    public void receive(Function<String,String> f) {
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
        ReceiveWorker currentWorker = worker;
        worker = new ReceiveWorker(requestConsumer, receiveChannel, currentWorker.getFunction());
        listener.execute(worker);
        currentWorker.shutdown();
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
                    logger.error("asyncSendConnect: {}", e);
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
                    logger.error("asyncReceiveReconnect: {}", e);
                    asyncReceiveReconnect(5);
                }
            }
        }, delay, TimeUnit.SECONDS);
    }
}
