package cn.rtmap.bigdata.ingest.wx.shake;

import java.io.IOException;
import cn.rtmap.bigdata.ingest.utils.URLUtil;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TokenManager {
	private String wsURL;
	private String result;
	
	public TokenManager(String accessTokenURL) {
		this.wsURL = accessTokenURL;
	}

	public String getToken() throws IOException {
		result = URLUtil.doGet(wsURL);
		if (result != null) {
			return parseToken(result);
		}
		return null;
	}
	
	private String parseToken(String json) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		AccessToken tok = mapper.readValue(json, AccessToken.class);
		if (tok != null) {
			return tok.getAccessToken();
		} else {
			return null;
		}
	}

//	public static void main(String[] args) throws IOException {
//		TokenManager tokmgr = new TokenManager("http://weix.rtmap.com/mp/wxb5e69065eb3d67ce/token");
//		String token = tokmgr.getToken();
//		System.out.println(tokmgr.getToken());
//		
//		String url = String.format("%s%s", "https://api.weixin.qq.com/shakearound/statistics/page?access_token=",token);
//		System.out.println(url);
//		
//		PageStatisticsParams p = new PageStatisticsParams();
//		p.setPage_id(1560044);
//		p.setBegin_date(1452297600);
//		p.setEnd_date(1452470400);
//
//		String result = URLUtil.doHttpsPost(url, p.toString());
//		System.out.println(result);
//	}
}
