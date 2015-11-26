package au.org.ala.layers.util;

import au.org.ala.layers.dto.Layer;
import au.org.ala.layers.intersect.IntersectConfig;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

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

    public static void updateDisplayPaths(List<Layer> layers) {
        if (layers == null) {
            return;
        }

        for (Layer layer : layers) {
            updateDisplayPath(layer);
        }
    }

    public static void updateMetadataPaths(List<Layer> layers) {
        if (layers == null) {
            return;
        }

        for (Layer layer : layers) {
            updateMetadataPath(layer);
        }
    }

    public static void updateDisplayPath(Layer layer) {
        if (layer.getDisplaypath() != null) {
            if (!layer.getDisplaypath().startsWith("/")) {
                layer.setDisplaypath(layer.getDisplaypath().replace(IntersectConfig.GEOSERVER_URL_PLACEHOLDER, IntersectConfig.getGeoserverUrl()));
            } else {
                layer.setDisplaypath(IntersectConfig.getGeoserverUrl() + layer.getDisplaypath());
            }
        }
    }

    public static void updateMetadataPath(Layer layer) {
        if (layer.getMetadatapath() != null) {
            if (!layer.getMetadatapath().startsWith("/")) {
                layer.setMetadatapath(layer.getMetadatapath().replace(IntersectConfig.GEONETWORK_URL_PLACEHOLDER, IntersectConfig.getGeonetworkUrl()));
            } else {
                layer.setMetadatapath(IntersectConfig.getGeonetworkUrl() + layer.getMetadatapath());
            }
        }
    }
}
