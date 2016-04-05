package com.trinetx.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import io.dropwizard.Configuration;

public class EnvironmentConfiguration extends Configuration {

    @JsonProperty
    private Map<String, ServiceSetting> serviceSettings;
    
    @JsonIgnore
    public ServiceSetting getOverrideSetting() {
        return serviceSettings.get(System.getProperty("env"));
    }
}
