package com.trinetx.core.messaging;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Created by Wai Cheng on 2/25/16.
 */
public class RpcQueueSetting {

    private static final String c_HostEnv = "{host}";

    private static RpcQueueSetting s_Setting = null;

    @JsonProperty
    private String Host;
    @JsonProperty
    private int    Port;
    @JsonProperty
    private String User;
    @JsonProperty
    private String Password;

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

    RpcQueueSetting() {
        /**
         * Hack -- only one RpcQueueSetting for each application
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

    public static void init(String hostname) {
        s_Setting.MyHost = hostname;
        resolveEnv(s_Setting.Queues);
    }

    public static void apply(final ConnectionFactory factory) {
        factory.setHost(s_Setting.Host);
        if (s_Setting.Port > 0)
            factory.setPort(s_Setting.Port);
        factory.setUsername(s_Setting.User);
        factory.setPassword(s_Setting.Password);
    }

    public static String getQueue(String queueId) {
        if (s_Setting == null || s_Setting.Queues == null)
            return null;
        return queueId.isEmpty() ? queueId : s_Setting.Queues.get(queueId);
    }
    
    public static Map<String, String> getQueues() {
        return s_Setting.Queues;
    }
    /*
     * Used only in unit test to perform what normally done by dropwizard
     */
    static void init(String host, int port, String user, String password, Map<String,String> queues) {
        s_Setting = new RpcQueueSetting();
        s_Setting.Host = host;
        s_Setting.Port = port;
        s_Setting.User = user;
        s_Setting.Password = password;
        s_Setting.Queues = queues;
    }

}
