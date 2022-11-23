package org.iro.reprex.camelmanualcommit;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.TemplatedRouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(SpringRunner.class)
@Import(org.iro.reprex.camelmanualcommit.KafkaTestContainersLiveTest.KafkaTestContainersConfiguration.class)
@SpringBootTest(classes = KafkaProducerConsumerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
public class KafkaTestContainersLiveTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestContainersLiveTest.class);

	@ClassRule
	public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"));

	@Autowired
	private KafkaProducer producer;

	@Autowired
	private EchoController controller;

	@Value("${test.topic}")
	private String topic;

	@LocalServerPort
	private int serverPort;

	private CamelContext camelContext = new DefaultCamelContext();

	private TemplateBuilder templateBuilder;

	@Before
	public void beforeTest() throws Exception {
		LOGGER.info("HTTP Server port : " + serverPort);

		templateBuilder = new TemplateBuilder(topic, kafka.getBootstrapServers(), serverPort);
		camelContext.addRoutes(templateBuilder);
		// @formatter:off
		TemplatedRouteBuilder
			.builder(camelContext, Constants.TEMPLATE_ID)
			.parameter(Constants.TEMPLATE_PARAM_PUBLISHER_ID, "pub-1")
			.routeId("route-1")
			.add();
		TemplatedRouteBuilder
			.builder(camelContext, Constants.TEMPLATE_ID)
			.parameter(Constants.TEMPLATE_PARAM_PUBLISHER_ID, "pub-2")
			.routeId("route-2")
			.add();
		// @formatter:on

		camelContext.start();
	}

	public void after() {
		camelContext.stop();
	}

	@Test
	public void givenKafkaDockerContainer_whenSendingWithSimpleProducer_thenHttpIsPostedAndKafkaOffsetCommited()
			throws Exception {
		// given
		// when
		String data = "{\"action\":\"PERSIST_DATA\",\"persistOptions\":{\"responsePayload\":\"SUMMARY\",\"persistMode\":\"ALWAYS\",\"optionsPerEntity\":{\"ActiviteEtablissement\":{\"enrichers\":[],\"validations\":[]},\"APourQualiteExercee\":{\"enrichers\":[],\"validations\":[]},\"AnnuaireSnv2\":{\"enrichers\":[],\"validations\":[]},\"PeriodeLocationGerance\":{\"enrichers\":[],\"validations\":[]},\"APourGroupeProfessionnel\":{\"enrichers\":[],\"validations\":[]},\"PeriodeEmbauche\":{\"enrichers\":[],\"validations\":[]},\"StatutIndividu\":{\"enrichers\":[],\"validations\":[]},\"RelationIndividu\":{\"enrichers\":[],\"validations\":[]},\"Redevabilite\":{\"enrichers\":[],\"validations\":[]},\"Individu\":{\"enrichers\":[],\"validations\":[]},\"Entreprise\":{\"enrichers\":[],\"validations\":[]},\"Etablissement\":{\"enrichers\":[],\"validations\":[]}},\"missingIdBehavior\":\"GENERATE\"},\"persistRecords\":{\"Entreprise\":[{\"PublisherID\":\"REI\",\"CodeNafSource\":\"4399C\",\"CdTypeEntrepriseSource\":\"P\",\"CdFormeJuridiqueSource\":\"1000\",\"CdSrc\":\"S417\",\"DtCessation\":\"2009-06-19\",\"DtCreation\":\"2004-03-08\",\"DtMaj\":\"2010-09-01T00:00:00.000Z\",\"Denomination\":\"ORCELLET  FRANCK  LAURENT  RJ  TRAVAUX  DE  MACONNERIE\",\"Siren\":\"452332315\",\"AdresseSiege.BureauDistributeur\":\"MAULAN\",\"AdresseSiege.LibelleVoie\":\"DE  NANT  LE  GRAND\",\"AdresseSiege.ComplementGeographique\":\"LD  LE  CHALET\",\"AdresseSiege.NumVoie\":\"6\",\"AdresseSiege.CdTypeVoieSource\":\"RUE\",\"AdresseSiege.CdPostalCedexSource\":\"55500\",\"AdresseSiege.CdCommuneSource\":\"55326\",\"AdresseSiege.CdValiditeAdresseSource\":\"3\",\"AdresseControle.BureauDistributeur\":\"MAULAN\",\"AdresseControle.LibelleVoie\":\"DE  NANT  LE  GRAND\",\"AdresseControle.ComplementGeographique\":\"LD  LE  CHALET\",\"AdresseControle.NumVoie\":\"6\",\"AdresseControle.CdTypeVoieSource\":\"RUE\",\"AdresseControle.CdPostalCedexSource\":\"55500\",\"AdresseControle.CdCommuneSource\":\"55326\"}]}}";

		// On attend, pour laisser le temps à Camel de démarrer et enregistrer les
		// consommateurs.
		Thread.sleep(1500);

		producer.send(topic, data);

		// then
		boolean messageConsumed = controller.getLatch().await(10, TimeUnit.SECONDS);

		boolean offsetCommited = templateBuilder.getCommitLatch().await(10, TimeUnit.SECONDS);

		assertTrue(messageConsumed);
		assertTrue(offsetCommited);
	}

	@TestConfiguration
	static class KafkaTestContainersConfiguration {

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "baeldung");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			return props;
		}

		@Bean
		public ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = new HashMap<>();
			configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
			configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(configProps);
		}

		@Bean
		public KafkaTemplate<String, String> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}
	}
}