package florian.siepe.entity.graph;

import org.jgrapht.graph.DefaultEdge;

public class Edge extends DefaultEdge {
    public EdgeType type;

    public static Edge of(EdgeType type) {
        final var edge = new Edge();
        edge.type = type;
        return edge;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Edge edge = (Edge) o;

        if (getSource() != null ? !getSource().equals(edge.getSource()) : edge.getSource() != null) return false;
        return getTarget() != null ? getTarget().equals(edge.getTarget()) : edge.getTarget() == null;
    }

    @Override
    public int hashCode() {
        int result = getSource() != null ? getSource().hashCode() : 0;
        result = 31 * result + (getTarget() != null ? getTarget().hashCode() : 0);
        return result;
    }
}
