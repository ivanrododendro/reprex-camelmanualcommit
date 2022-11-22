package org.iro.reprex.camelmanualcommit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.TemplatedRouteBuilder;
import org.apache.camel.main.ConfigureRouteTemplates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CkRouteTemplates implements ConfigureRouteTemplates {

	private static final Logger LOGGER = LoggerFactory.getLogger(CkRouteTemplates.class);

	public void configure(CamelContext context) {
		LOGGER.info("Building route");

		TemplatedRouteBuilder.builder(context, "test").add();
	}

}
