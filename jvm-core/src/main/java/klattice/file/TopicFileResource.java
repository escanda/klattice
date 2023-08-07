package klattice.file;

import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Param;

import java.io.ByteArrayInputStream;

@ApplicationScoped
@Path("/topic/")
public class TopicFileResource {
    @LoggerName("TopicFileResource")
    Logger logger;

    @Inject
    Files fs;

    @GET
    @Path("{topic}")
    public Response get(@Param(String.class) String topic) {
        logger.infov("Serving topic file {0}", topic);
        var topicBytes = fs.get(topic);
        if (topicBytes.isEmpty()) {
            return Response.noContent().build();
        } else {
            return Response.ok(new ByteArrayInputStream(topicBytes.get())).build();
        }
    }
}
