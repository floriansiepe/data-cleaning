package florian.siepe.control.graph;

import florian.siepe.entity.graph.Edge;
import florian.siepe.entity.graph.EdgeType;
import florian.siepe.entity.graph.Vertex;
import florian.siepe.entity.graph.VertexType;
import org.jgrapht.graph.SimpleDirectedGraph;

import javax.enterprise.context.ApplicationScoped;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

@ApplicationScoped
public class GraphFactory {
    public SimpleDirectedGraph<Vertex<String>, Edge> buildClassHierarchy(File hierarchy) {
        final var graph = new SimpleDirectedGraph<Vertex<String>, Edge>(null, () -> Edge.of(EdgeType.SUBCLASS), false);

        try (var br = new BufferedReader(new FileReader(hierarchy))) {
            String line;
            while ((line = br.readLine()) != null) {
                final var split = line.split("\\s+");
                final var subClass = split[0];
                final var parentClass = split[1];
                final var subV = Vertex.of(subClass, VertexType.CLASS);
                final var parentV = Vertex.of(parentClass, VertexType.CLASS);
                graph.addVertex(subV);
                graph.addVertex(parentV);
                graph.addEdge(subV, parentV);
            }
            return graph;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
