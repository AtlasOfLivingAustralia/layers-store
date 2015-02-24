package au.org.ala.layers.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by a on 29/01/15.
 */
public class Util {
    private static final Logger LOGGER = Logger.getLogger(Util.class);

    public static String readUrl(String feature) {
        StringBuilder content = new StringBuilder();

        HttpURLConnection conn = null;
        try {
            // Construct data

            // Send data
            URL url = new URL(feature);
            conn = (HttpURLConnection) url.openConnection();
            conn.connect();

            // Get the response
            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                content.append(line);
            }

        } catch (Exception e) {
            LOGGER.error("failed to read URL: " + feature);
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (Exception e) {
                    LOGGER.error("failed to close url: " + feature, e);
                }
            }
        }
        return content.toString();
    }
}
