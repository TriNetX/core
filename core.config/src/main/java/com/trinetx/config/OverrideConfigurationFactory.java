package com.trinetx.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.validation.Validator;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkNotNull;

import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationSourceProvider;

public class OverrideConfigurationFactory<T> extends ConfigurationFactory<T> {
    private final static String ENVIRONMENTS_PATH = "config/environments.yml";
    private final ObjectMapper mapper;

    public OverrideConfigurationFactory(Class<T> klass, Validator validator, ObjectMapper objectMapper, String propertyPrefix) {
        super(klass, validator, objectMapper, propertyPrefix);
        this.mapper = objectMapper.copy();
    }

    @Override
    public T build(ConfigurationSourceProvider provider, String path) throws IOException, ConfigurationException {
        T config = super.build(provider, path);
        // gracefully handle generic config class does not implement Overridable
        if (config instanceof OverridableConfig) {
            try {
                InputStream input = provider.open(checkNotNull(ENVIRONMENTS_PATH));
                final JsonNode node = mapper.readTree( new YAMLFactory().createParser(input));
                EnvironmentConfiguration env = mapper.readValue(new TreeTraversingParser(node), EnvironmentConfiguration.class);
                ServerOverrideSetting overrides = env.getOverrideSetting(System.getProperty("env"));
                if(overrides == null) {
                    System.out.println("No overrides was found for environment: " + System.getProperty("env"));
                } else {
                    ((OverridableConfig)config).setOverrides(overrides);
                }
            } catch (IOException e) {
                //ignore as environments.yml may not exist
            }
        }
        System.out.println("Configuration Settings: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config));
        return config;
    }
}
