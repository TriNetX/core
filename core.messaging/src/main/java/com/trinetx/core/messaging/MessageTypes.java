package com.trinetx.core.messaging;

public final class MessageTypes {
    
    public class TERM_SERVER {
        public static final String COMMAND = "command";
        public static final String TERM_RESOURCE = "term_resource";
        public static final String TERM_COUNT_RESOURCE = "term_count_resource";
        public static final String AGE_VALUES_RESOURCE = "age_values_resource";
        public static final String AGE_COUNTS_RESOURCE = "age_counts_resource";
        public static final String CONCEPT_HIERARCHY_RESOURCE = "concept_hierarchy_resource";
        public static final String CONCEPT_HIERARCHY_CHILDREN_RESOURCE = "concept_hierarchy_children_resource";
        public static final String CONCEPT_COUNTS_RESOURCE = "concept_counts_resource";
        public static final String CONCEPT_COUNTS_RESOURCE_ConceptCountsRequest = "concept_counts_request"; // TODO: move to TSParam once it is move out of pubsub
        public static final String LAB_VALUES_RESOURCE = "lab_values_resource";
    }
    
}
