package backend;

import static spark.Spark.get;
import static spark.SparkBase.setPort;
import static spark.SparkBase.externalStaticFileLocation;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Request;
import spark.Response;
import spark.SparkBase;

public class Main {

	static Logger logger = LoggerFactory.getLogger(Main.class);

	private ESNodeManager esNodeManager;

	private ServiceIndex serviceIndex = new ServiceIndex();

	public static void main(String[] args) throws Exception {
		new Main().start(args);
	}

	private void start(String[] args) throws Exception {

		final String dataDir = (args.length > 1) ? args[0] : "data";
		esNodeManager = new ESNodeManager(dataDir);
		esNodeManager.start();                


		setPort(8080);
		route();

		createShutdownHook();
	}

	private void route() {
		externalStaticFileLocation("html");

		get("/api", (req, res) -> {
			producesJson(res);
			return serviceIndex.renderIndex();
		});

		new ESDocumentResource(esNodeManager, "lib", "book", "/api/")
		.createRoutes(serviceIndex);

		new ESDocumentResource(esNodeManager, "lib", "author", "/api/")
		.createRoutes(serviceIndex);	
	}

	private void producesJson(Response res) {
		res.type("application/json");
	}

	private void createShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				logger.info("bsbackend graceful shutdown ..");
				new Thread(new Runnable() {
					public void run() {
						try {
							esNodeManager.stop();
							SparkBase.stop();
						} catch (Exception e) {
							logger.error("error on stopping server", e);
						}
					}
				}).start();

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}

				logger.info("backend halt(0)");
				Runtime.getRuntime().halt(0);
			}
		});
	}
}
