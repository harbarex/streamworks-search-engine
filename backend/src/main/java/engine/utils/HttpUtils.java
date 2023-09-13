package engine.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class HttpUtils {
    public static HttpURLConnection sendRequest(String dest, String reqType, String path, String parameters) throws IOException {
		URL url = new URL(dest + "/" + path);

        System.out.println("Sending request to: " + url.toString());

		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setRequestMethod(reqType);
		
		if (reqType.equals("POST")) {
            conn.setDoOutput(true);
			conn.setRequestProperty("Content-Type", "application/json");
			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		}
		return conn;
    }

    public static String readResponse(HttpURLConnection conn) throws IOException {
        InputStream inputStream = conn.getInputStream();
        int bufferSize = 1024;
        char[] buffer = new char[bufferSize];
        StringBuilder out = new StringBuilder();
        Reader in = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        for (int numRead; (numRead = in.read(buffer, 0, buffer.length)) > 0; ) {
            out.append(buffer, 0, numRead);
        }
        return out.toString();
    }
}
