package org.iro.reprex.camelmanualcommit;

import java.util.concurrent.CountDownLatch;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemplateBuilder extends RouteBuilder {

	private static final Logger LOGGER = LoggerFactory.getLogger(RouteBuilder.class);

	private String topic;

	private String bootstrapServers;

	private int httpServerPort;

	private CountDownLatch commitLatch = new CountDownLatch(1);

	public TemplateBuilder(String topic, String bootstrapServers, int httpServerPort) {
		this.topic = topic;
		this.bootstrapServers = bootstrapServers;
		this.httpServerPort = httpServerPort;
	}

	public CountDownLatch getCommitLatch() {
		return commitLatch;
	}

	public void setCommitLatch(CountDownLatch commitLatch) {
		this.commitLatch = commitLatch;
	}

	public void configure() {
		LOGGER.info("Building template");

		String endpoint = "kafka:" + topic + "?allowManualCommit=true&autoCommitEnable=false&brokers="
				+ bootstrapServers;

		// @formatter:off
		routeTemplate(Constants.TEMPLATE_ID)
			.templateParameter(Constants.TEMPLATE_PARAM_PUBLISHER_ID)
			.from(endpoint)
			.messageHistory()
			 	.onCompletion().onFailureOnly()
		        .to("log:sync")
		    .end()
				.onCompletion().onCompleteOnly()
		        .process(new KafkaOffsetProcessor())
	        .end()
			.log("Message received")
			.filter(simple("${header.publisherId} == '{{"+Constants.TEMPLATE_PARAM_PUBLISHER_ID+"}}'"))
			.process(new BusinessProcessor())
			.throttle(1).timePeriodMillis(1000).asyncDelayed(true)
			.setHeader(Exchange.HTTP_METHOD, simple("POST"))
			.setHeader("Content-type", constant("application/json;charset=UTF-8"))
			.setHeader("Accept",constant("application/json"))
			.resequence(header(Constants.DML_TIMESTAMP_HEADER)).batch().timeout(100)
			.to("http://localhost:" + httpServerPort + "/echo-post");
		// @formatter:on
	}

	private final class KafkaOffsetProcessor implements Processor {
		@Override
		public void process(Exchange exchange) throws Exception {
			String routeId = exchange.getFromRouteId();
			KafkaManualCommit manual = exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT,
					KafkaManualCommit.class);
			manual.commit();
			log.info("Committed Kafka offset from route [{}]", routeId);
			commitLatch.countDown();
		}
	}

	private final class BusinessProcessor implements Processor {
		@Override
		public void process(Exchange exchange) throws Exception {
			String routeId = exchange.getFromRouteId();
			LOGGER.info("Processing message from route [{}]", routeId);
		}
	}

}