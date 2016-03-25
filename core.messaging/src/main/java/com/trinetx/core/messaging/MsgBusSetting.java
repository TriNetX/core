package com.trinetx.core.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Map;

/**
 * Created by yongdengchen on 7/2/14.
 */
public class MsgBusSetting {

    private static final String c_HostEnv = "{host}";

    private static MsgBusSetting s_Setting = null;

    @JsonProperty
    private String Host;
    @JsonProperty
    private int    Port;
    @JsonProperty
    private String User;
    @JsonProperty
    private String Password;

    /**
     * { ExchangeID, ExchangeName } pair
     *
     * ExchangeName may contain '{host}' placement holder to be replaced with the actual
     * hostname of the machine -- so, 2 dev sessions from 2 people do not interfere with each other
     *
     * ExchangeName shall be deployment mode specific -- so, test deployment does not interfere with
     * staging deployment, etc.
     */
    @JsonProperty
    private Map<String, String> Exchanges;

    /**
     * { QueueId, QueueName } pair
     *
     * QueueName may contain '{host}' placement holder to be replaced with the actual
     * hostname of the machine -- so, 2 dev sessions from 2 people do not interfere with each other
     *
     * QueueName may also be deployment mode specific -- so, test deployment does not interfere with
     * staging deployment, etc.
     */
    @JsonProperty
    private Map<String, String> Queues;

    private String              MyHost;

    MsgBusSetting() {
        /**
         * Hack -- only one MsgBusSetting for each application
         */
        s_Setting = this;
    }

    private static void resolveEnv(final Map<String, String> map) {
        for (final Map.Entry<String, String> pair : map.entrySet()) {
            String val = pair.getValue();
            if (val.contains(c_HostEnv)) {
                val = val.replace(c_HostEnv, s_Setting.MyHost);
                pair.setValue(val);
            }
        }
    }

    static void init(String hostname) {
        s_Setting.MyHost = hostname;
        resolveEnv(s_Setting.Exchanges);
        resolveEnv(s_Setting.Queues);
    }

    static void apply(final ConnectionFactory factory) {
        factory.setHost(s_Setting.Host);
        if (s_Setting.Port > 0)
            factory.setPort(s_Setting.Port);
        factory.setUsername(s_Setting.User);
        factory.setPassword(s_Setting.Password);
    }

    public static String getExchange(String exchangeId) {
        return exchangeId.isEmpty() ? exchangeId : s_Setting.Exchanges.get(exchangeId);
    }

    public static String getQueue(String queueId) {
        if (s_Setting == null || s_Setting.Queues == null)
            return null;
        return queueId.isEmpty() ? queueId : s_Setting.Queues.get(queueId);
    }
}
