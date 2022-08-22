package florian.siepe.blocker;

import florian.siepe.entity.transformer.BlockingCandidateResponse;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.Collection;
import java.util.List;

@RegisterRestClient(baseUri = "http://localhost:5000")
public interface TransformerRestClient {
    @POST
    @Path("predict")
    <T> List<BlockingCandidateResponse> predict(Collection<T> samples, @QueryParam("k") int k);
}
