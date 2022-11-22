package org.iro.reprex.camelmanualcommit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(SpringRunner.class)
@Import(org.iro.reprex.camelmanualcommit.KafkaTestContainersLiveTest.KafkaTestContainersConfiguration.class)
@SpringBootTest(classes = KafkaProducerConsumerApplication.class)
@DirtiesContext
public class KafkaTestContainersLiveTest {

	@ClassRule
	public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

	@Autowired
	private KafkaConsumer consumer;

	@Autowired
	private KafkaProducer producer;

	@Value("${test.topic}")
	private String topic;

	@Test
	public void givenKafkaDockerContainer_whenSendingWithSimpleProducer_thenMessageReceived() throws Exception {
		String data = "Sending with our own simple KafkaProducer";

		producer.send(topic, data);

		boolean messageConsumed = consumer.getLatch().await(10, TimeUnit.SECONDS);

		assertTrue(messageConsumed);
		assertThat(consumer.getPayload(), containsString(data));
	}

	class KafkaTestContainersConfiguration {

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "baeldung");
			return props;
		}

		@Bean
		public ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = new HashMap<>();
			configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
			return new DefaultKafkaProducerFactory<>(configProps);
		}
	}
}