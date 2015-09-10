package backend;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ES client manager creating a local es node. Here we start an embedded elastic
 * search server. For security reasons, this elasicsearch does not open the http
 * port, if not wanted.
 * 
 */
public class ESNodeManager {

    /**
     * the logger.
     */
    private final Logger logger = LoggerFactory.getLogger(ESNodeManager.class);

    /**
     * The elasticsearch Node instance.
     */
    private Node esNode;

    private String dataDir;

    /**
     * Creates a new local node.
     * 
     * @param cfg the global service configuration
     */
    public ESNodeManager(String dataDir) {
	this.dataDir = dataDir;
    }

    public void start() throws Exception {
        logger.info("starting new elasticsearch in data dir: "+ dataDir);
	
        Settings settings = ImmutableSettings.builder()
	    .put("path.data", dataDir)
	    .put("network.host", "127.0.0.1")
	    .put("http.enabled", true)
	    .build();
	
        esNode = new NodeBuilder()
	    .local(true)
	    .clusterName("example-cluster")
	    .settings(settings)
	    .node();
    }

    public void stop() throws Exception {
        logger.info("stoping Elasticsearch");
        esNode.close();
    }
    
    public Client client() {
        return esNode.client();
    }
    
    public boolean doesIndexExist(String index) {
        return esNode.client().admin().indices().prepareExists(index)
	    .execute().actionGet().isExists();	
    }
}
