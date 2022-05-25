package florian.siepe.control.io;

import java.util.concurrent.atomic.AtomicInteger;

public class GlobalIdProvider {
    private final AtomicInteger ids = new AtomicInteger();

    public int get() {
        return ids.getAndIncrement();
    }
}
