package com.trinetx.core.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;

/**
 * Created by yongdengchen on 7/2/14.
 * <p/>
 * The listener (Consumer)
 */
public abstract class Subscriber {
    protected Channel channel = null;

    protected void setChannel(Connection connection) throws IOException {
        if (channel != null) {
            try {
                channel.close();
            }
            catch (final Exception e) {
            }
            channel = null;
        }
        channel = connection.createChannel();
    }

    public abstract void listen(Connection connection) throws IOException;
}
