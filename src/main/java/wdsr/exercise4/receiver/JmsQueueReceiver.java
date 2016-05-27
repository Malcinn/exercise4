package wdsr.exercise4.receiver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
	
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	
	private final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");
	
	private Connection connection = null;
	
	private Session session = null;
	
	private Destination destination = null;
	
	private MessageConsumer messageConsumer = null;
	
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	
	public JmsQueueReceiver(final String queueName) {
		try {
			/* I have no idea which classes add to trustedPackages, so I allowed all of them ;/
			String[] arrayOfTrustedPackages = {"wdsr.exercise4","java.math.BigDecimal", "wdsr.exercise4.receiver"};
			this.connectionFactory.setTrustedPackages(new ArrayList<String>(Arrays.asList(arrayOfTrustedPackages)));
			*/
			this.connectionFactory.setTrustAllPackages(true);
			this.connection = connectionFactory.createConnection();
			this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.destination = this.session.createQueue(queueName);
			this.messageConsumer = this.session.createConsumer(destination, "JMSType = 'PriceAlert' OR JMSType = 'VolumeAlert'");
			this.startUp();
		} catch (JMSException ex) {
			log.error("Caught : "+ ex);
			ex.printStackTrace();
		}
	}
	
	private void startUp() throws JMSException{
		if (this.connection != null && this.session != null)
			this.connection.start();
	}
	
	private void close() throws JMSException{
		if (this.connection != null && this.session != null){
			this.messageConsumer.close();
			this.session.close();
			this.connection.close();
		}
	}

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) {
		// TODO
		try {
			MessageListener messageListener = new MyMessageListener(alertService);
			this.messageConsumer.setMessageListener(messageListener);
		} catch (JMSException ex) {
			log.error("Caught : "+ ex);
			ex.printStackTrace();
		}
	}
	
	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		try {
			this.close();
		} catch (JMSException ex) {
			log.error("Caught : "+ ex);
			ex.printStackTrace();
		}
	}

	// TODO
	// This object should start consuming messages when registerCallback method is invoked.
	
	// This object should consume two types of messages:
	// 1. Price alert - identified by header JMSType=PriceAlert - should invoke AlertService::processPriceAlert
	// 2. Volume alert - identified by header JMSType=VolumeAlert - should invoke AlertService::processVolumeAlert
	// Use different message listeners for and a JMS selector 
	
	// Each alert can come as either an ObjectMessage (with payload being an instance of PriceAlert or VolumeAlert class)
	// or as a TextMessage.
	// Text for PriceAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Price=<long value>
	// Text for VolumeAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Volume=<long value>
	
	// When shutdown() method is invoked on this object it should remove the listeners and close open connection to the broker.   
}
