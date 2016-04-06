package com.trinetx.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rabbitmq.client.ConnectionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Wai Cheng on 2/25/16.
 */
public class RpcQueueSetting implements Overridable<RpcQueueSetting> {
    private final String DEFAULT_HOST = "";
    private final int DEFAULT_PORT = -1;
    private final String DEFAULT_USER = "";
    private final String DEFAULT_PASSWORD = "";
    private final Map<String, String> DEFAULT_QUEUES = new HashMap<String, String>();

    private static final String c_HostEnv = "{host}";

    private static RpcQueueSetting s_Setting = null;

    @JsonProperty
    private String Host = DEFAULT_HOST;
    @JsonProperty
    private int    Port = DEFAULT_PORT;
    @JsonProperty
    private String User = DEFAULT_USER;
    @JsonProperty
    private String Password = DEFAULT_PASSWORD;

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
    private Map<String, String> Queues = DEFAULT_QUEUES;

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
    public static void init(String host, int port, String user, String password, Map<String,String> queues) {
        s_Setting = new RpcQueueSetting();
        s_Setting.Host = host;
        s_Setting.Port = port;
        s_Setting.User = user;
        s_Setting.Password = password;
        s_Setting.Queues = queues;
    }

    @Override
    public void override(Overridable<RpcQueueSetting> o) {
        RpcQueueSetting s = (RpcQueueSetting) o;
    
        if (!DEFAULT_HOST.equals(s.Host)) {
            this.Host = s.Host;
        }
        if (DEFAULT_PORT != s.Port) {
            this.Port = s.Port;
        }
        if (!DEFAULT_USER.equals(s.User)) {
            this.User = s.User;
        }
        if (!DEFAULT_PASSWORD.equals(s.Password)) {
            this.Password = s.Password;
        }
        // even queues are not override, then they would just be empty maps so no harm is calling putAll()
        this.Queues.putAll(s.Queues);
    }
}
