package florian.siepe.control.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.EnumMapSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import de.javakaffee.kryoserializers.*;

import java.lang.reflect.InvocationHandler;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

public enum KryoFactory {
    ;

    public static Kryo createKryoInstance() {
        final var kryo = new KryoReflectionFactorySupport() {

            @Override
            @SuppressWarnings({"rawtypes", "unchecked"})
            public Serializer<?> getDefaultSerializer(Class type) {
                if (EnumSet.class.isAssignableFrom(type)) {
                    return new DefaultSerializers.EnumSetSerializer();
                }
                if (EnumMap.class.isAssignableFrom(type)) {
                    return new EnumMapSerializer(type);
                }
                if (Collection.class.isAssignableFrom(type)) {
                    return new CopyForIterateCollectionSerializer();
                }
                if (Map.class.isAssignableFrom(type)) {
                    return new CopyForIterateMapSerializer();
                }
                if (Date.class.isAssignableFrom(type)) {
                    return new DefaultSerializers.DateSerializer();
                }
                if (TimeZone.class.isAssignableFrom(type)) {
                    return new DefaultSerializers.TimeZoneSerializer();
                }
                return super.getDefaultSerializer(type);
            }
        };
        kryo.setRegistrationRequired(false);
        kryo.setReferences(true);
        kryo.register(List.of("").getClass(), new DefaultSerializers.ArraysAsListSerializer());
        kryo.register(Collections.emptyList().getClass(), new DefaultSerializers.CollectionsEmptyListSerializer());
        kryo.register(Collections.emptyMap().getClass(), new DefaultSerializers.CollectionsEmptyMapSerializer());
        kryo.register(Collections.emptySet().getClass(), new DefaultSerializers.CollectionsEmptySetSerializer());
        kryo.register(Collections.singletonList("").getClass(), new DefaultSerializers.CollectionsSingletonListSerializer());
        kryo.register(Collections.singleton("").getClass(), new DefaultSerializers.CollectionsSingletonSetSerializer());
        kryo.register(Collections.singletonMap("", "").getClass(), new DefaultSerializers.CollectionsSingletonMapSerializer());
        kryo.register(BigDecimal.class, new DefaultSerializers.BigDecimalSerializer());
        kryo.register(BigInteger.class, new DefaultSerializers.BigIntegerSerializer());
        kryo.register(Pattern.class, new RegexSerializer());
        kryo.register(BitSet.class, new DefaultSerializers.BitSetSerializer());
        kryo.register(URI.class, new URISerializer());
        kryo.register(UUID.class, new UUIDSerializer());
        kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
        kryo.register(InvocationHandler.class, new JdkProxySerializer());
        kryo.register(KnowledgeBase.class);
        kryo.register(KnowledgeIndex.class);
        kryo.register(com.google.common.collect.HashBiMap.class, new JavaSerializer());
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);
        return kryo;
    }
}
