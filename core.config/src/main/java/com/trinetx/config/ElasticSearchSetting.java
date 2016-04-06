package com.trinetx.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Elastic Search setting
 *
 * Created by yongdengchen on 7/28/14.
 */
public class ElasticSearchSetting implements Overridable<ElasticSearchSetting> {
    private final String DEFAULT_CLUSTER_NAME = "";
    private final String DEFAULT_SERVER_HOST = "";
    private final int DEFAULT_NGRAM_LENGTH = -1;
    
    @JsonProperty
    private String clusterName = DEFAULT_CLUSTER_NAME;

    @JsonProperty
    private String serverHost = DEFAULT_SERVER_HOST;

    @JsonProperty
    private int ngramLength = DEFAULT_NGRAM_LENGTH;

    @JsonIgnore
    public String getClusterName() {
        return clusterName;
    }

    @JsonIgnore
    public String getServerHost() {
        return serverHost;
    }

    @JsonIgnore
    public int getNGramLength() {
        return ngramLength;
    }

    @Override
    public void override(Overridable<ElasticSearchSetting> o) {
        ElasticSearchSetting s = (ElasticSearchSetting) o;
        if (!DEFAULT_CLUSTER_NAME.equals(s.clusterName)) {
            this.clusterName = s.clusterName;
        }
        if (!DEFAULT_SERVER_HOST.equals(s.serverHost)) {
            this.serverHost = s.serverHost;
        }
        if (DEFAULT_NGRAM_LENGTH != s.ngramLength) {
            this.ngramLength = s.ngramLength;
        }
    }
}
