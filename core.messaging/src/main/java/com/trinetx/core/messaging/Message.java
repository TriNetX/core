package com.trinetx.core.messaging;

/**
 * Message object (internal) -- for publishing / sending 'data' via message bus.
 *
 * If exchangeId is empty, queue must not be empty.
 *
 * Created by yongdengchen on 7/2/14.
 */
class Message {
    String exchangeName;
    String queueName;
    byte[] data;

    Message(final String exchangeId, final String queue, final byte[] data) {
        exchangeName = MsgBusSetting.getExchange(exchangeId);
        queueName = queue;
        this.data = data;
    }
}
