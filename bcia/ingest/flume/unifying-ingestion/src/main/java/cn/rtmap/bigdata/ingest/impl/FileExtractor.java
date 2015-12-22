package cn.rtmap.bigdata.ingest.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.utils.FileManager;

public class FileExtractor implements Extractor {
	static final Logger logger = LoggerFactory.getLogger(FileExtractor.class);
	
	String fileName;
	FileManager mgr;
	long rowId;

	@Override
	public void init(Context ctx) {
		mgr = new FileManager(ctx);
	}

	@Override
	public boolean prepare() {
		rowId = 0;
		fileName = mgr.nextFile();
		return (fileName != null);
	}

	@Override
	public Iterator<JsonElement<String, String>> getData() {
		List<JsonElement<String, String>> list = new LinkedList<JsonElement<String, String>>();
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String line;
			while ((line = br.readLine()) != null) {
				++rowId;
				JsonElement<String, String> o = new JsonElement<String, String>();
				o.setHeaders(mgr.getFileHeader());
			    o.addHeader(HeaderConstants.DEF_ROW_NUM, String.valueOf(rowId));
				o.setBody(line.getBytes());
				list.add(o);
			}
		} catch (IOException e) {
			logger.error("Read file content failed", e);
		}

		if (rowId > 0) {
			JsonElement<String, String> c = new JsonElement<String, String>();
			c.setHeaders(mgr.getFileHeader());
			c.setHeader(HeaderConstants.DEF_MODE, HeaderConstants.VAL_MODE_FIN);
			c.addHeader(HeaderConstants.DEF_ROW_NUM, String.valueOf(rowId));
			list.add(c);
		}
		return list.iterator();
	}

	@Override
	public void cleanup() {
		mgr.backupFile();
	}

/*
	public static void main(String[] args) throws InterruptedException {
		Extractor extractor = new FileExtractor();
		extractor.init(null);

		while (extractor.prepare()) {
			try {
				Iterator<JsonElement<String, String>> it = extractor.getData();
				while (it.hasNext()) {
					JsonElement<String, String> o = it.next();
					System.out.println(new String(o.getBody()));
				}
			} finally {
				extractor.cleanup();
			}
			Thread.sleep(1000);
		}
	}
*/
}
