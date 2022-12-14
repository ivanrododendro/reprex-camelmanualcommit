package org.iro.reprex.camelmanualcommit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void send(String topic, String payload) {
		LOGGER.info("sending payload='{}' to topic='{}'", payload, topic);
		// @formatter:off
		Message<String> message = MessageBuilder.withPayload(payload)
				.setHeader(KafkaHeaders.TOPIC, topic)
				.setHeader(Constants.TEMPLATE_PARAM_PUBLISHER_ID, "pub-1")
				.setHeader(Constants.DML_TIMESTAMP_HEADER, String.valueOf(System.currentTimeMillis()))
				.build();
		// @formatter:on

		kafkaTemplate.send(message);
	}
}