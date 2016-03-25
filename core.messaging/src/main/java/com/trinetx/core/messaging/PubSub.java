package com.trinetx.core.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created by yongdengchen on 7/2/14.
 */
public class PubSub {

    private static ShutdownListener s_ShutdownListener;

    static {
        s_ShutdownListener = new ShutdownListener() {
            @Override
            public void shutdownCompleted(final ShutdownSignalException cause) {
                s_Logger.info("====== PubSub: shutdown: " + cause.getMessage());
                s_BusConnection = null;
                asyncConnect(5);
            }
        };
    }

    private static Logger s_Logger = null;

    private static ArrayList<Message> s_MessageQueue = new ArrayList<>();
    private static ConnectionFactory s_ConnectionFactory;
    private static Connection s_BusConnection = null;

    public static void init() throws IOException {
        if (s_Logger == null) {
            MsgBusSetting.init(Hostname.getMyName());
            s_Logger = LoggerFactory.getLogger(PubSub.class);
            connect();
        }
    }

    public static void enqueueMsg(final String queue, final byte[] data) {
        if (queue != null)
            publish("", queue, data);
    }

    public static synchronized void publish(final String exchangeId, final String queueName, final byte[] data) {
        s_MessageQueue.add(new Message(exchangeId, queueName, data));
        if (s_BusConnection == null) {
            // non-blocking connect
            asyncConnect(0);
        }
        else
            publishQueuedMessages();
    }

    public static byte[] pollMsg(final String queue, int timeout) throws IOException, InterruptedException {
        Channel          channel  = s_BusConnection.createChannel();
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queue, false, consumer);
        QueueingConsumer.Delivery delivery = consumer.nextDelivery(timeout);

        byte[] msg;
        if (delivery != null) {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            msg = delivery.getBody();
        }
        else {
            msg = null;
        }

        if (channel != null) {
            try {
                channel.close();
            }
            catch (final Exception e) {
            }
        }

        return msg;
    }

    static synchronized void connect() throws IOException {
        if (s_BusConnection != null) {
            // already connected
            return;
        }

        s_Logger.info("====== PubSub: connecting");
        s_ConnectionFactory = new ConnectionFactory();
        MsgBusSetting.apply(s_ConnectionFactory);
        s_ConnectionFactory.setConnectionTimeout(2500);

        try {
            s_BusConnection = s_ConnectionFactory.newConnection();
            s_Logger.info("====== PubSub: connected");
        }
        catch (final Exception e) {
            s_Logger.error("Connect: {}", e);
            s_BusConnection = null;
            asyncConnect(5);
            return;
        }
        s_BusConnection.addShutdownListener(s_ShutdownListener);

        Subscription.subscribe(s_BusConnection);
        publishQueuedMessages();
    }

    private static void publishQueuedMessages() {
        if (s_MessageQueue.isEmpty())
            return;

        Channel channel = null;
        try {
            channel = s_BusConnection.createChannel();
        }
        catch (final Exception e) {
            return;
        }

        final Iterator<Message> i = s_MessageQueue.iterator();
        while (i.hasNext()) {
            final Message msg = i.next();
            try {
                channel.basicPublish(msg.exchangeName, msg.queueName, MessageProperties.PERSISTENT_BASIC, msg.data);
            }
            catch (final Exception e) {
                s_Logger.error("Publish: {}", e);
                break;
            }
            i.remove();
        }
        if (channel != null) {
            try {
                channel.close();
            }
            catch (final Exception e) {
            }
        }
    }

    private static void asyncConnect(final int delay) {
        Scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    connect();
                }
                catch (final Exception e) {
                    s_Logger.error("AsyncConnect: {}", e);
                    s_BusConnection = null;
                    asyncConnect(5);
                }
            }
        }, delay, TimeUnit.SECONDS);
    }
}
