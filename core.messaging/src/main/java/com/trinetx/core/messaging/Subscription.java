package com.trinetx.core.messaging;

import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by yongdengchen on 7/3/14.
 */
public class Subscription {

    private static ArrayList<Subscriber> subscribers = new ArrayList<>(10);
    private static Connection s_Connection = null;

    public static synchronized void subscribe(Connection connection) throws IOException {
        s_Connection = connection;
        for (final Subscriber s : subscribers)
            if (s != null)
                s.listen(connection);
    }

    public static synchronized void register(Subscriber subscriber) throws IOException {
        if (s_Connection != null)
            subscriber.listen(s_Connection);
        subscribers.add(subscriber);
    }
}
