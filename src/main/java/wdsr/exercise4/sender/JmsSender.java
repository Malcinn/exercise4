package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.hamcrest.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
	
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	
	private final String topicName;

	// Create connection factory
	private final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");
	
	private Connection connection = null;
	
	private Session session = null;
	
	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
		try {
			this.connection = connectionFactory.createConnection();
			this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
			this.session.close();
			this.connection.close();
		}
	}
	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		// send order to queue as object message
		try{
			this.startUp();
			
			// Create destination - Queue
			Destination destinationQueue = this.session.createQueue(this.queueName);
			// Create message producer from session to queue
			MessageProducer messageProducer = this.session.createProducer(destinationQueue);
			messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			// Create object message
			ObjectMessage objectMessage = session.createObjectMessage(new Order(orderId, product, price));
			objectMessage.setJMSType("Order");
			objectMessage.setStringProperty("WDSR-System", "OrderProcessor");
			// Invoke the producer send method
			messageProducer.send(objectMessage);
			
			this.close();
		} catch (Exception ex){
			log.error("Caught : "+ ex);
			ex.printStackTrace();
		}
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) {
		try {
			this.startUp();
			
			// Creates destination - queue
			Destination destinationQueue = this.session.createQueue(this.queueName);
			// Creates message producer from session to destination(queue)
			MessageProducer messageProducer = this.session.createProducer(destinationQueue);
			// Creates message - TextMessage
			Message textMessage = this.session.createTextMessage(text);
			// Invoke the producer send method
			messageProducer.send(textMessage);
			
			this.close();
		} catch (JMSException ex) {
			log.error("Caught : "+ ex);
			ex.printStackTrace();
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		try {
			this.startUp();
			
			// Create destination - topic
			Destination destinationTopic = this.session.createTopic(this.topicName);
			// Create Message Producer form session to topic
			MessageProducer messageProducer = this.session.createProducer(destinationTopic);
			// Create message - MapMessage
			MapMessage mapMessage = this.session.createMapMessage();
			for (Map.Entry<String, String> entry : map.entrySet()){
				mapMessage.setString(entry.getKey(), entry.getValue());
			}
			// Invoke the producer send method
			messageProducer.send(mapMessage);
			
			this.close();
		} catch (JMSException ex) {
			log.error("Caught : "+ ex);
			ex.printStackTrace();
		}
	}
}
