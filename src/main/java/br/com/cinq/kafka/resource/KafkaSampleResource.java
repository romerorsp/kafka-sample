package br.com.cinq.kafka.resource;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import br.com.cinq.kafka.sample.Producer;

/**
 * Greet Service
 *
 * @author Adriano Kretschmer
 */
@Path("/kafka")
public class KafkaSampleResource {
	Logger logger = LoggerFactory.getLogger(KafkaSampleResource.class);

	// For now, store the information on a bean, in memory
	@Autowired
	@Qualifier("sampleProducer")
	Producer sampleProducer;

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response send(String message) {
		try {
			logger.info("Sending message {}", message);

			sampleProducer.send(message);

		} catch (Exception e) {
			logger.error("An exception occurred during Greet message update", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("exception").build();
		}

		return Response.ok().build();
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("/{repeat}")
	public Response sendBatch(@PathParam("repeat") int repeat, String message) {
		try {
			logger.info("Sending message - repeat {} {}", message, repeat);

			for(int i=0;i<repeat;i++) {
				sampleProducer.send(message.replace("{count}","{"+i+"}"));
			}

		} catch (Exception e) {
			logger.error("An exception occurred during Greet message update", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("exception").build();
		}

		return Response.ok().build();
	}

}
