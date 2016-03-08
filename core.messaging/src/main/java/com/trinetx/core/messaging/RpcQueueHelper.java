package com.trinetx.core.messaging;

/**
 * Simple helper class to keep RpcQueue agnostic to services.  Important to note that null can be returned.
 * 
 * @author wai.cheng
 *
 */
public class RpcQueueHelper {
    private static final String TERM_SERVER_QUEUE_NAME = "termServerQueue";
  
    public static final RpcQueue getTermServerQueue() {
        return RpcQueue.getQueue(TERM_SERVER_QUEUE_NAME);
    }
   
}
