package com.trinetx.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ServerOverrideSetting {

    @JsonProperty
    private ElasticSearchSetting elasticSearch;

    @JsonProperty
    private MongoSetting mongo;

    @JsonProperty
    private MsgBusSetting msgBusSetting;

    @JsonProperty
    private List<String> loadProviders;

    public ElasticSearchSetting getElasticSearchSetting() {
        return elasticSearch;
    }

    public MongoSetting getMongoSetting() {
        return mongo;
    }

    public MsgBusSetting gteMsgBusSetting() {
        return msgBusSetting;
    }

    public List<String> getLoadProviders() {
        return loadProviders;
    }
}
