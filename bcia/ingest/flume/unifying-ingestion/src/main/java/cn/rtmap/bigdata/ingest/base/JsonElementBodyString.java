package cn.rtmap.bigdata.ingest.base;

import java.util.HashMap;
import java.util.Map;

public class JsonElementBodyString<K, V> {
	private Map<K, V> headers = new HashMap<K, V>();
	private String body;

	public void addHeader(K k, V v) {
		headers.put(k, v);
	}

	public void setHeader(K k, V v) {
		addHeader(k, v);
	}

	public void setBody(String data) {
		body = data;
	}

	public byte[] getBody() {
		return body.getBytes();
	}

	public Object getHeader(K key) {
		return headers.get(key);
	}

	public void setHeaders(Map<K, V> headers) {
		this.headers = headers;
	}

	public Map<K, V> getHeaders() {
		return headers;
	}
}
