package com.trinetx.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.validation.Validator;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public T build(ConfigurationSourceProvider provider, String path) throws IOException, ConfigurationException {
        T config = super.build(provider, path);
        try {
            InputStream input = provider.open(ENVIRONMENTS_PATH);
            final JsonNode root = (JsonNode) mapper.readTree( new YAMLFactory().createParser(input)).get(System.getProperty("env"));
            if (root != null) {
                // start reading and applying overrides
                Field[] fields = config.getClass().getDeclaredFields();
                for (Field f : fields) {
                    if (f.isAnnotationPresent(Overridden.class)) {
                        // found overridden fields, attempt to read and apply override values if they exist
                        JsonNode node = root.get(f.getName());
                        if (node != null) {
                            // only handles List and Overridable field classes
                            if(List.class.isAssignableFrom(f.getType()))
                            {
                                List overrides = (List) mapper.readValue(new TreeTraversingParser(node), ArrayList.class);
                                // force private fields to be accessible and apply overrides 
                                f.setAccessible(true);
                                f.set(config, overrides);
                            } else {
                                Overridable overrides = (Overridable) mapper.readValue(new TreeTraversingParser(node), f.getType().newInstance().getClass());
                                // force private fields to be accessible and apply overrides 
                                f.setAccessible(true);  
                                Overridable settings = (Overridable) f.get(config);
                                settings.override(overrides);
                            }
    
                        }
                    }
                }
            }
        } catch (IOException|InstantiationException|IllegalAccessException e) {
            // gracefully handle any error applying override, print and ignore
            System.out.println("Exception caught in applying overridden settings in \"" + ENVIRONMENTS_PATH + "\" to file \"" + path + "\"");
        }
        System.out.println("Configuration Settings: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config));
        return config;
    }
}
