package au.org.ala.layers.dto;

import java.util.List;
import java.util.Map;

public class StoreRequest {

    private String apiKey;
    private String layersServiceUrl;
    private List<String> filter;
    private List<Map> exclude;
    private List<String> include;

    public StoreRequest() {
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public List<String> getFilter() {
        return filter;
    }

    public void setFilter(List<String> filter) {
        this.filter = filter;
    }

    public List<Map> getExclude() {
        return exclude;
    }

    public void setExclude(List<Map> exclude) {
        this.exclude = exclude;
    }

    public List<String> getInclude() {
        return include;
    }

    public void setInclude(List<String> include) {
        this.include = include;
    }

    public String getLayersServiceUrl() {
        return layersServiceUrl;
    }

    public void setLayersServiceUrl(String layersServiceUrl) {
        this.layersServiceUrl = layersServiceUrl;
    }
}
