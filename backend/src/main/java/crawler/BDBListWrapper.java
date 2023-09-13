package crawler;

import java.io.Serializable;
import java.util.List;

/**
 * Wrapper class for storing adjacency list in Berkeley DB
 * @author Daniel
 *
 */
public class BDBListWrapper implements Serializable {
    
    private static final long serialVersionUID = -6923510496160143611L;
    public List<String> fromEdges;

    public BDBListWrapper(List<String> fromEdges) {
        this.fromEdges = fromEdges;
    }

}
