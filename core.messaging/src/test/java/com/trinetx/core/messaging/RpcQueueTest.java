package com.trinetx.core.messaging;

import com.google.common.io.Files;

import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

// support class for creating an embedded QPid AMQP broker
class EmbeddedAMQPBroker {
    public static final int BROKER_PORT = 5672;	// rabbitmq default broker port
    private final Broker broker = new Broker();
    public EmbeddedAMQPBroker() throws Exception {
        final String configFileName = "qpid-config.json";
        final String passwordFileName = "passwd.properties";
        // prepare options
        final BrokerOptions brokerOptions = new BrokerOptions();
        brokerOptions.setConfigProperty("qpid.amqp_port", String.valueOf(BROKER_PORT));
        brokerOptions.setConfigProperty("qpid.pass_file", findResourcePath(passwordFileName));
        brokerOptions.setConfigProperty("qpid.work_dir", Files.createTempDir().getAbsolutePath());
        brokerOptions.setInitialConfigurationLocation(findResourcePath(configFileName));
        // start broker
        broker.startup(brokerOptions);
    }
    
    private String findResourcePath(final String file) throws IOException {
    	return getClass().getClassLoader().getResource(file).getPath();
    }
}
	
public class RpcQueueTest {
	private static final String QUEUE_NAME = "test-queue";
	// test username and password must match ones in test/resources/passwd.properties file
	private static final String QUEUE_USERNAME = "guest";
	private static final String QUEUE_PASSWORD = "guest";
	
	private RpcQueue queue;
	
    @SuppressWarnings("serial")
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
    	// create embedded QPid message broker and initialize RpcQueueSetting
		new EmbeddedAMQPBroker();
		RpcQueueSetting.init(
				"0.0.0.0", 0, QUEUE_USERNAME, QUEUE_PASSWORD,
				new HashMap<String,String>(){{put(QUEUE_NAME, "{host}_test_queue");}});
		RpcQueue.init();
		RpcQueue.sendConnect();
		RpcQueue.receiveConnect();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		RpcQueue.shutdown();
	}

	@Before
	public void setUp() throws Exception {
		// create and send up test queue to be a sender and receiver
		queue = RpcQueue.getQueue(QUEUE_NAME);
		queue.receive((s)->s); // echo received message
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSendReceive() throws Exception {
		String message = "if you give a mouse a cookie";
		assertEquals("Message received from echo queue does not match one sent",
				message, new String(queue.send(message.getBytes())));
	}

	@Test
	public void testConcurrency() throws Exception {
		Map<String, String> results = new HashMap<String,String>();
		for (int i=0;i<1000;i++) {
			final int ii = i;
	        Thread t = new Thread(new Runnable() {
	    		String message = "if you give a mouse " + ii + " cookies";
				@Override
				public void run() {
					try {
						results.put(message, new String(queue.send(message.getBytes())));
					} catch (Exception e) {
					}
				}
			});
	        t.start();
	        t.join();
		}
		
		// assert key and value pairs match in result
		results.forEach((k,v)->assertEquals("Message received from echo queue does not match one sent", k,v));
	}

}
