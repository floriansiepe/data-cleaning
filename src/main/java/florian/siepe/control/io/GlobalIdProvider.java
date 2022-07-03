package florian.siepe.control.io;

import java.io.Serializable;

public class GlobalIdProvider implements Serializable {
    private int ids = 0;

    public synchronized int get() {
        return ids++;
    }
}
