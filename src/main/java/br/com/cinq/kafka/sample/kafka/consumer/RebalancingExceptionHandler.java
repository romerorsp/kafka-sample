package br.com.cinq.kafka.sample.kafka.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ExceptionHandler;

@Component
public class RebalancingExceptionHandler {
	
	@ExceptionHandler(value=org.apache.kafka.clients.consumer.CommitFailedException.class)
	public void handle(CommitFailedException e) {
		System.out.println("Rebalancing..... " + e.getLocalizedMessage());
	}
}
