package florian.siepe.entity.graph;

import java.util.Objects;

public class Vertex<T> {
    public T id;
    public VertexType type;

    public static <T> Vertex<T> of(T id, VertexType label) {
        final var vertex = new Vertex<T>();
        vertex.id = id;
        vertex.type = label;
        return vertex;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Vertex<?> vertex = (Vertex<?>) o;

        return Objects.equals(id, vertex.id);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return id.toString();
    }
}
