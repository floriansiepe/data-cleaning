package florian.siepe.control.io;

import java.io.Serializable;

public class GlobalIdProvider implements Serializable {
    private int ids;

    public synchronized int get() {
        int i = this.ids;
        this.ids++;
        return i;
    }
}
