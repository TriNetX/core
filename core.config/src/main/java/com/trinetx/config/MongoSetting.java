package com.trinetx.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Mongo URL
 *
 * Created by yongdengchen on 8/28/14. Moving it from Term Server to Pubsub.
 */
public class MongoSetting implements Overridable<MongoSetting> {
    private final String DEFAULT_URL = "";

    @NotNull
    @JsonProperty
    private String URL = DEFAULT_URL;

    @JsonIgnore
    public String getURL() {
        return URL;
    }

    @Override
    public void override(Overridable<MongoSetting> o) {
        MongoSetting s = (MongoSetting) o;
        if (!DEFAULT_URL.equals(s.URL)) {
            this.URL = s.URL;
        }
    }
}
