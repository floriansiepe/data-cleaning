package florian.siepe.control.graph;

import florian.siepe.entity.graph.Edge;
import florian.siepe.entity.graph.Vertex;
import org.jgrapht.Graph;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;

import java.io.StringWriter;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

public class GraphUtils {
    public static String writeDot(Graph<Vertex<String>, Edge> graph) {

        DOTExporter<Vertex<String>, Edge> exporter =
                new DOTExporter<>(v -> v.id.replace('.', '_'));
        exporter.setVertexAttributeProvider((v) -> {
            Map<String, Attribute> map = new LinkedHashMap<>();
            map.put("label", DefaultAttribute.createAttribute(v.toString()));
            return map;
        });
        Writer writer = new StringWriter();
        exporter.exportGraph(graph, writer);
        return writer.toString();
    }
}
