package backend;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This is a generic implementation for a rest resource which supports crud
 * operations for one document in an index of the elasticsearch. It can be
 * customized by subclassing.
 * 
 * @author smancke
 */
public class ESDocumentResource {

	private static final int DEFAULT_FETCH_SIZE = 1000;

	/**
	 * the logger.
	 */
	private final Logger logger = LoggerFactory.getLogger(ESDocumentResource.class);

	/**
	 * The eslasticsearch type for documents.
	 */
	private String type;

	/**
	 * The elasticsearch index name.
	 */
	private String index;

	/**
	 * The elasticsearch client manager instance.
	 */
	private ESNodeManager es;

	private String basePath;

	private String creationIdPath;

	private int notFoundReturnCode = 404;

	/**
	 * Create a new resource.
	 * 
	 * @param esNode the access to elasticsearch
	 */
	public ESDocumentResource(final ESNodeManager esNode, String index, String type, String basePath) {
		this.es = esNode;
		this.index = index;
		this.type = type;
		this.basePath = basePath;
	}

	/**
	 * Creates the routes for this resource.
	 * @param serviceIndex Added the route descriptions to the service index description document.
	 * 
	 * @param The basepath with a trailing slash, if needed, e.g. "/"
	 */
	public void createRoutes(ServiceIndex serviceIndex) {

		String listAllURI = basePath + index + "/" + type;
		get(listAllURI,
				(req, res) -> {
					producesJson(res);
					return listAll();
				});
		serviceIndex.add(new ServiceIndex.Service(listAllURI, index + " " + type));

		get(basePath + index + "/" + type + "/:id",
				(req, res) -> {
					producesJson(res);
					String object = getOneById(req.params("id"));
					if (object == null) {
						res.status(notFoundReturnCode);
						return "";
					}
					return object;
				});

		put(basePath + index + "/" + type + "/_import",
				(req, res) -> importDocuments(req.body()));

		delete(basePath + index + "/" + type + "/_deleteAll",
				(req, res) -> deleteAll());

		delete(basePath + index + "/" + type + "/:id",
				(req, res) -> deleteOne(req.params("id")));

		put(basePath + index + "/" + type + "/:id",
				(req, res) -> updateOne(req.params("id"), req.body()));

		post(basePath + index + "/" + type,
				(req, res) -> {
					String id = create(req.body());
					res.header("Location", idToLocation(id));
					res.status(201);
					producesJson(res);
					return getOneById(id);
				});
	}

	public String listAll() {
		if (!es.doesIndexExist(index)) {
			logger.warn("index " + index + " does not exist.");
			return "[]";
		}
		logger.trace("searching for all " + type + " in index: " + index + ".");

		SearchRequestBuilder search = createSearchRequest(null, null);
		SearchResponse response = doSearch(search);
		return responseToJsonList(response);
	}

	public String create(final String content) {
		logger.trace("create document of type " + type + " in index: " + index + ".");
		String id = null;
		if (getCreationIdPath() != null) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				JsonNode object = mapper.readTree(content);
				JsonNode idNode = object.findPath(getCreationIdPath());
				id = idNode.asText();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		IndexResponse resp = es.client().prepareIndex(index, type, id)
				.setSource(content).execute().actionGet();
		logger.trace("have created document of type " + type + " in index: " + index + ", got id: " + resp.getId());
		refreshIndexes();
		return resp.getId();
	}

	public String getOneById(final String id) {
		if (!es.doesIndexExist(index)) {
			logger.warn("index " + index + " does not exist.");
			return null;
		}
		logger.trace("fetching one entry from index " + index + " and type " + type + " with id " + id + " in es");
		return es.client().prepareGet(index, type, id).execute()
				.actionGet().getSourceAsString();
	}

	public String updateOne(final String id, final String content) {
		logger.trace("update one entry from index " + index + " and type " + type + " with id " + id + " in es");
		es.client().prepareIndex(index, type, id).setSource(content)
		.execute().actionGet();
		return "updated.\n";
	}

	public String deleteOne(final String id) {
		logger.trace("delete one entry from index " + index + " and type " + type + " with id " + id + " in es");
		es.client().prepareDelete(index, type, id).execute().actionGet();
		refreshIndexes();
		return "deleted.\n";
	}


	public Object deleteAll() {
		logger.trace("delete all documents from index " + index + " and type " + type + " in es");
		SearchRequestBuilder search = es.client().prepareSearch()
				.setIndices(index).setTypes(type)
				.setFetchSource(new String[0], null)
				.setSize(DEFAULT_FETCH_SIZE);         

		SearchResponse response = doSearch(search);

		int counter = 0;
		for (SearchHit hit : response.getHits().getHits()) {
			es.client().prepareDelete(index, type, hit.getId()).execute().actionGet(); 	 
			counter++;
		}

		return counter + " documents deleted.\n";
	}

	/**
	 * Imports an array of documents.
	 * Each document is stored wit the supplied id from the field '@id'.
	 * If a document already exists, it will be overwritten.
	 * 
	 * @param content json srting with array of objects
	 * @return
	 */
	public String importDocuments(String content) {
		ObjectMapper mapper = new ObjectMapper();
		int count = 0;
		try {
			JsonNode object = mapper.readTree(content);
			if (! object.isArray()) {
				throw new RuntimeException("json document is not an array");            	
			}
			ArrayNode rootNode = (ArrayNode)object;
			for (JsonNode node : rootNode) {
				if (!node.isObject()) {
					throw new RuntimeException("json child is not an object");    
				}
				ObjectNode objectNode = (ObjectNode)node;

				String id = null;
				if (objectNode.get("@id") != null) {
					id = objectNode.get("@id").asText();
					objectNode.remove("@id");
				} else if (getCreationIdPath() != null) {
					JsonNode idNode = objectNode.findPath(getCreationIdPath());
					id = idNode.asText();
					if (id == null) {
						throw new RuntimeException("can not determine id (creationIdPath="+getCreationIdPath()+") id for "+ objectNode);
					}
				}
				es.client().prepareIndex(index, type, id).setSource(objectNode.toString())
				.execute().actionGet();
				count++;
			}           
		} catch (IOException e) {
			throw new RuntimeException("can not parse json content for import", e);
		}
		return count +" documents imported.\n";
	}

	protected SearchRequestBuilder createSearchRequest(final String[] fields, String seachTerm) {
		SearchRequestBuilder search = es.client().prepareSearch()
				.setIndices(index).setTypes(type)
				.setFetchSource(fields, null)
				.setSize(DEFAULT_FETCH_SIZE);

		if (seachTerm != null && seachTerm.trim().length() > 0) {
			search.setQuery(QueryBuilders.queryString(seachTerm));
		}
		return search;
	}

	protected SearchResponse doSearch(SearchRequestBuilder search) {
		SearchResponse response = search.execute().actionGet();
		if (response.getHits().getHits().length < response.getHits().getTotalHits()) {
			search.setSize((int) response.getHits().getTotalHits());
			response = search.execute().actionGet();
		}
		return response;
	}

	/**
	 * Creates a json list of object, each containing an id and source attribute
	 * 
	 * @param response the es search response
	 * @return a lis of cvs as map of strings and maps
	 */
	protected String responseToJsonList(SearchResponse response) {
		ObjectMapper objectMapper = new ObjectMapper();    	
		ArrayNode rootNode = objectMapper.createArrayNode();

		for (SearchHit hit : response.getHits().getHits()) {
			JsonNode node;
			try {
				node = objectMapper.readTree(hit.getSourceAsString());			
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			ensureId(hit, node);
			rootNode.add(node);
		}

		return rootNode.toString();
	}

	private void ensureId(SearchHit hit, JsonNode node) {
		if (getCreationIdPath() == null) {
			if (node.isObject()) {
				ObjectNode objectNode = (ObjectNode)node;
				objectNode.put("@id", hit.getId());
			}
		}
	}

	protected void refreshIndexes() {
        es.client().admin().indices().prepareRefresh().execute().actionGet();
    }

	private void producesJson(Response res) {
		res.type("application/json; charset=utf-8");
	}

	private String idToLocation(String id) {
		return basePath + index + "/" + type + "/" + id;
	}

	public String getCreationIdPath() {
		return creationIdPath;
	}

	public ESDocumentResource setCreationIdPath(String creationIdPath) {
		this.creationIdPath = creationIdPath;
		return this;
	}

	public ESDocumentResource setNotFoundReturnCode(int statusCode) {
		notFoundReturnCode = statusCode;
		return this;
	}

	public int getNotFoundReturnCode() {
		return notFoundReturnCode;
	}
}
