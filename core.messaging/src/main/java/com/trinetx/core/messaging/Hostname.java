package com.trinetx.core.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by yongdengchen on 8/6/14.
 */
public class Hostname {
    private static final Logger s_Logger = LoggerFactory.getLogger(Hostname.class);

    private static String s_MyName = "unknown";

    static {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            s_MyName = addr.getHostName();
            int index = s_MyName.indexOf('.');
            if (index > 0)
                s_MyName = s_MyName.substring(0, index);
        }
        catch (UnknownHostException e) {
        }
        s_Logger.info("Hostname is '{}'", s_MyName);
    }

    /**
     * Get this machine's hostname
     *
     * @return hostname
     */
    public static String getMyName() {
        return s_MyName;
    }
}
