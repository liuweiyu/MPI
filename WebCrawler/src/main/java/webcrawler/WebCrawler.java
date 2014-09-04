package webcrawler;

import java.io.IOException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.*;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.net.URL;

public class WebCrawler {
	public static Set<String> oldUrls = new HashSet<String>();	
	public static String ROOT = "http://www.utah.edu/";
	public static String DOMAIN = "utah.edu";
	public static String TOPIC = "admission";
	public static String SUFFIX = ".pdf";
	public static String BACK_SLASH = "/";
	public static String FILE_PATH = ".\\data\\";
	public static String ROBOT_SUFFIX = "/robots.txt";
	
	public static void main(String[] args){		
		oldUrls.add(ROOT);		
		Set<String> curUrls = new HashSet<String>();
		curUrls.add(ROOT);
		try{
			Fetcher(curUrls);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void Fetcher(Set<String> curUrls) throws IOException{	
		Set<String> newUrls = new HashSet<String>();
		Iterator<String> it = curUrls.iterator();
		while(it.hasNext()){
			String tmpUrl = it.next();
			if(tmpUrl.endsWith(SUFFIX)){
				try{
					SaveFileFromUrl(tmpUrl);
				}catch(IOException e){
					e.printStackTrace();
				}
			}else{
				try{
					Document doc = Jsoup.connect(tmpUrl).get();
					Elements questions = doc.select("a[href]");				
					
					for(Element link: questions){
						String tmpAbsUrl = link.attr("abs:href");
						tmpAbsUrl = tmpAbsUrl.toLowerCase();
						String tmpPossibleRobotUrl = tmpUrl + ROBOT_SUFFIX;
						if(tmpAbsUrl.startsWith("http://") && tmpAbsUrl.contains(DOMAIN) 
							&& tmpAbsUrl.contains(TOPIC) && !oldUrls.contains(tmpAbsUrl) 
							&& !tmpAbsUrl.equalsIgnoreCase(tmpPossibleRobotUrl)){		
							newUrls.add(tmpAbsUrl);
							oldUrls.add(tmpAbsUrl);
							System.out.println(tmpAbsUrl);
						}
					}		
				}catch(IOException e){
					e.printStackTrace();
					continue;
				}						
			}
		}
		Fetcher(newUrls);
	}
	
	public static void SaveFileFromUrl(String fileUrl) throws IOException {
		int lastBackSlashIndex = fileUrl.lastIndexOf(BACK_SLASH);
		int length = fileUrl.length();
		String fileName = fileUrl.substring(lastBackSlashIndex+1, length);
		fileName = FILE_PATH + fileName;
		
		BufferedInputStream in = null;
		FileOutputStream fout = null;
		try {
			in = new BufferedInputStream(new URL(fileUrl).openStream());
			fout = new FileOutputStream(fileName);
			 
			byte data[] = new byte[1024];
			int count;
			while ((count = in.read(data, 0, 1024)) != -1) {
				fout.write(data, 0, count);
			}
		} finally {
			if (in != null)
			in.close();
			if (fout != null)
			fout.close();
		}
	}
}
