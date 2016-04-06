package com.trinetx.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.rabbitmq.client.ConnectionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yongdengchen on 7/2/14.
 */
public class MsgBusSetting implements Overridable<MsgBusSetting> {
    private final String DEFAULT_HOST = "";
    private final int DEFAULT_PORT = -1;
    private final String DEFAULT_USER = "";
    private final String DEFAULT_PASSWORD = "";
    private final Map<String, String> DEFAULT_EXCHANGES = new HashMap<String, String>();
    private final Map<String, String> DEFAULT_QUEUES = new HashMap<String, String>();

    private static final String c_HostEnv = "{host}";

    private static MsgBusSetting s_Setting = null;

    @JsonProperty
    private String Host = DEFAULT_HOST;
    @JsonProperty
    private int    Port = DEFAULT_PORT;
    @JsonProperty
    private String User = DEFAULT_USER;
    @JsonProperty
    private String Password = DEFAULT_PASSWORD;

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
    private Map<String, String> Exchanges = DEFAULT_EXCHANGES;

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

    public static void init(String hostname) {
        s_Setting.MyHost = hostname;
        resolveEnv(s_Setting.Exchanges);
        resolveEnv(s_Setting.Queues);
    }

    public static void apply(final ConnectionFactory factory) {
        factory.setHost(s_Setting.Host);
        if (s_Setting.Port > 0)
            factory.setPort(s_Setting.Port);
        factory.setUsername(s_Setting.User);
        factory.setPassword(s_Setting.Password);
    }

    @JsonIgnore
    public static String getExchange(String exchangeId) {
        return exchangeId.isEmpty() ? exchangeId : s_Setting.Exchanges.get(exchangeId);
    }

    @JsonIgnore
    public static String getQueue(String queueId) {
        if (s_Setting == null || s_Setting.Queues == null)
            return null;
        return queueId.isEmpty() ? queueId : s_Setting.Queues.get(queueId);
    }

    @Override
    public void override(Overridable<MsgBusSetting> o) {
        MsgBusSetting s = (MsgBusSetting) o;
    
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
        // even exchanges and queues are not override, then they would just be empty maps so no harm is calling putAll()
        this.Exchanges.putAll(s.Exchanges);
        this.Queues.putAll(s.Queues);
    }
}
