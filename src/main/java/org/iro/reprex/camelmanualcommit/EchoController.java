package org.iro.reprex.camelmanualcommit;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/echo-post")
class EchoController {

	private static final Logger LOGGER = LoggerFactory.getLogger(EchoController.class);

	private CountDownLatch latch = new CountDownLatch(1);

	@PostMapping
	@ResponseStatus(code = org.springframework.http.HttpStatus.OK)
	public String create(@RequestBody String body) {
		LOGGER.info("Request body : {}", body);

		latch.countDown();

		return body;
	}

	public CountDownLatch getLatch() {
		return latch;
	}
}