package cn.rtmap.flume.source.sftp;

//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class DataFile {
	//private static final Logger LOG = LoggerFactory.getLogger(DataFile.class);

	private String fileName;
	private String filePath;
	private String md5sum;
	private String dataType;
	private String extName;
	private List<byte[]> data;
	
	private final static String DATA_TYPE_LOCATION = "location";
	private final static String DATA_TYPE_PASSENGER = "passenger";
	private final static String DATA_TYPE_EVENT = "event";
	private final static String DATA_TYPE_UNKNOWN = "unknown";

	public String getDataType() {
		if (dataType != null)
			return dataType;
		else {
			if (fileName.contains(DATA_TYPE_LOCATION)) {
				return DATA_TYPE_LOCATION;
			} else if (fileName.contains(DATA_TYPE_PASSENGER)) {
				return DATA_TYPE_PASSENGER;
			} else if (fileName.contains(DATA_TYPE_EVENT)) {
				return DATA_TYPE_EVENT;
			} else return DATA_TYPE_UNKNOWN;
		}
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		if (fileName.lastIndexOf(".") > 0) {
			this.fileName = fileName.substring(0, fileName.lastIndexOf("."));
			this.extName = fileName.substring(fileName.lastIndexOf("."), fileName.length());
		}
		else {
			this.fileName =  fileName;
			this.extName = "";
		}
	}

	public String getExtName() {
		return (extName != null) ? extName : "";
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getMd5Sum() {
		return md5sum;
	}

	public void setMd5Sum(String md5Sum) {
		this.md5sum = md5Sum;
	}

	/*public List<byte[]> getData() {
		return data;
	}*/

	public void setData(List<byte[]> data) {
		this.data = data;
	}

	public List<String[]> getAllRows() {
		List<String[]> allrows = new ArrayList<String[]>();
		
		int i = 0;
		HashMap<Integer, String> lastrows = new HashMap<Integer, String>();
		String[] toks = null;

		for (byte[] block : data) {
			String raw = new String(block);
			toks = raw.split("\n");
			
			int key = i - 1;
			int len = raw.endsWith("\n") ? toks.length : toks.length - 1;
			String last = lastrows.get(key);

			if (last != null && !last.isEmpty()) {
				String combinedStr = String.format("%s%s", last, toks[0]);
				allrows.add(combinedStr.split("_"));
				for (int j = 1; j < len; ++j) {
					allrows.add(toks[j].split("_"));
				}
			} else {
				for (int j = 0; j < len; ++j) {
					allrows.add(toks[j].split("_"));
				}
			}

			if (!raw.endsWith("\n")) {
				lastrows.put(i, toks[toks.length - 1]);
			}
			++i;
		}

		/*FileWriter fstream = null;
		try {
			fstream = new FileWriter(String.format("D:\\%s", fileName));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		  BufferedWriter out = new BufferedWriter(fstream);
		  try {
			for (String s : allrows)
				out.write(s);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  //Close the output stream
		  try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

		return allrows;
	}
	
	
	/*public static List<String> getAllRowsV2(List<byte[]> data2, String fileName) {
		List<String> allrows = new ArrayList<String>();
		
		int i = 0;
		HashMap<Integer, String> lastrows = new HashMap<Integer, String>();
		String[] toks = null;

		for (byte[] block : data2) {
			String raw = new String(block);
			toks = raw.split("\n");
			
			int len = toks.length;
			
			for (int j = 0; j < len; ++j) {
					allrows.add(toks[j]);
			}
		}

		FileWriter fstream = null;
		try {
			fstream = new FileWriter(String.format("D:\\%s", fileName));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		  BufferedWriter out = new BufferedWriter(fstream);
		  try {
			for (String s : allrows)
				out.write(s);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  //Close the output stream
		  try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return allrows;
	}*/
}
