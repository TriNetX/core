package com.trinetx.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import io.dropwizard.Configuration;

public class EnvironmentConfiguration extends Configuration {

    private static volatile EnvironmentConfiguration theConfig;

    @JsonProperty
    private Map<String,ServerOverrideSetting> overrides;

    public ServerOverrideSetting getOverrideSetting(String env) {
        return overrides.get(env);
    }
    
    public void publish() {
        theConfig = this;
    }

    public static EnvironmentConfiguration getInstance() {
        EnvironmentConfiguration config = theConfig;
        if (config == null) {
            throw new ExceptionInInitializerError("not yet published");
        }
        return config;
    }
}
