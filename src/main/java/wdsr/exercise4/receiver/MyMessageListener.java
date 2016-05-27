package wdsr.exercise4.receiver;

import java.math.BigDecimal;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;

public class MyMessageListener implements MessageListener {

	private AlertService alertService;

	public MyMessageListener(AlertService alertService) {
		this.alertService = alertService;
	}

	@Override
	public void onMessage(Message message) {
		try {
			if (message.getJMSType().equals("PriceAlert"))
				processPriceAlert(message);
			if (message.getJMSType().equals("VolumeAlert"))
				processVolumeAlert(message);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void processPriceAlert(Message message) throws JMSException {
		PriceAlert priceAlert = null;
		if (message instanceof ObjectMessage) {
			ObjectMessage objectMessage = (ObjectMessage) message;
			priceAlert = (PriceAlert) objectMessage.getObject();
		} else if (message instanceof TextMessage) {
			TextMessage textMessage = (TextMessage) message;
			String[] arrayOfLines = textMessage.getText().split("\n");
			if (arrayOfLines.length == 3) {
				long timestamp = Long.parseLong(arrayOfLines[0].substring(arrayOfLines[0].indexOf("=") + 1));
				String stock = arrayOfLines[1].substring(arrayOfLines[1].indexOf("=") + 1);
				BigDecimal currentPrice = new BigDecimal(
						(arrayOfLines[2].substring(arrayOfLines[2].indexOf("=") + 1)).trim());
				priceAlert = new PriceAlert(timestamp, stock, currentPrice);
			}
		}
		this.alertService.processPriceAlert(priceAlert);
	}

	private void processVolumeAlert(Message message) throws JMSException {
		VolumeAlert volumeAlert = null;
		if (message instanceof ObjectMessage) {
			ObjectMessage objectMessage = (ObjectMessage) message;
			volumeAlert = (VolumeAlert) objectMessage.getObject();
		} else if (message instanceof TextMessage) {
			TextMessage textMessage = (TextMessage) message;
			String[] arrayOfLines = textMessage.getText().split("\n");
			if (arrayOfLines.length == 3) {
				long timestamp = Long.parseLong(arrayOfLines[0].substring(arrayOfLines[0].indexOf("=") + 1));
				String stock = arrayOfLines[1].substring(arrayOfLines[1].indexOf("=") + 1);
				long floatingVolume = Long.parseLong(arrayOfLines[2].substring(arrayOfLines[2].indexOf("=") + 1));
				volumeAlert = new VolumeAlert(timestamp, stock, floatingVolume);
			}
		}
		this.alertService.processVolumeAlert(volumeAlert);
	}

}
