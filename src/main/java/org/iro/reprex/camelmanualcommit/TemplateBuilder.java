package org.iro.reprex.camelmanualcommit;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TemplateBuilder extends RouteBuilder {
	private static final Logger LOGGER = LoggerFactory.getLogger(CkRouteTemplates.class);

	@Value("${test.topic}")
	private String topic;

	public void configure() throws Exception {
		LOGGER.info("Building template");

		String endpoint = "kafka:" + topic + "?allowManualCommit=true&autoCommitEnable=false&brokers=localhost:9092";

		// @formatter:off
		routeTemplate("test")
			.from(endpoint)
			.log("Message received" )
			.errorHandler(deadLetterChannel("seda:error").useOriginalMessage())		
			.messageHistory()
			.process(new Processor() {
				@Override
				public void process(Exchange exchange) throws Exception {
					KafkaManualCommit manual = exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
					manual.commit();
					
					log.info("Committed Kafka offset");
				}
			});
		// @formatter:on
	}
}