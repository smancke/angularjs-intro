package backend;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ServiceIndex {

	List<Service> serviceURLs = new ArrayList<Service>();
	
	public List<Service> getServiceURLs() {
		return serviceURLs;
	}

	public String renderIndex() {	
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void setServiceURLs(List<Service> serviceURLs) {
		this.serviceURLs = serviceURLs;
	}

	public void add(Service service) {
		serviceURLs.add(service);
	}
	
	public static class Service {		
		String href;
		String title;
		
		public Service(String href, String title) {
			this.href = href;
			this.title = title;
		}

		public String getHref() {
			return href;
		}

		public void setHref(String href) {
			this.href = href;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}
	}
}
