package com.trinetx.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

import io.dropwizard.Configuration;

public class ServiceSetting extends Configuration {

    @JsonProperty
    private ElasticSearchSetting elasticSearch;

    @JsonProperty
    private MongoSetting mongo;

    @JsonProperty
    private MsgBusSetting msgBusSetting;

    @JsonProperty
    private List<String> loadProviders = new ArrayList<String>();

    @JsonIgnore
    public ElasticSearchSetting getElasticSearchSetting() {
        return elasticSearch;
    }

    @JsonIgnore
    public MongoSetting getMongoSetting() {
        return mongo;
    }

    @JsonIgnore
    public MsgBusSetting getMsgBusSetting() {
        return msgBusSetting;
    }

    @JsonIgnore
    public List<String> getLoadProviders() {
        return loadProviders;
    }
}
