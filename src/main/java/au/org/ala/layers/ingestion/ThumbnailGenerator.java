package au.org.ala.layers.ingestion;

import au.org.ala.layers.dto.Layer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URL;
import java.util.List;

/**
 * Created by a on 19/09/2014.
 */
public class ThumbnailGenerator {
    private static final Logger logger = Logger.getLogger(ThumbnailGenerator.class);

    private static final int THUMBNAIL_WIDTH = 200;
    private static final int THUMBNAIL_HEIGHT = 200;

    public static void many(List<Layer> layers, String thumbnailDir, boolean force) {

        //only updates missing thumbnails
        for (Layer layer : layers) {
            if (force || !hasThumbnail(layer.getName(), thumbnailDir)) {
                one(layer, thumbnailDir);
            }
        }
    }

    private static boolean hasThumbnail(String layerName, String thumbnailDir) {
        return new File(thumbnailFileName(layerName, thumbnailDir)).exists();
    }

    private static String thumbnailFileName(String layerName, String thumbnailDir) {
        return thumbnailDir + "/ALA:" + layerName + ".jpg";
    }

    public static void one(Layer layer, String thumbnailDir) {
        String geoserverUrl = layer.getDisplaypath();
        //trim from /wms or /gwc
        int wmsPos = geoserverUrl.indexOf("/wms");
        if (wmsPos >= 0) {
            geoserverUrl = geoserverUrl.substring(0, wmsPos);
        }
        int gwcPos = geoserverUrl.indexOf("/gwc");
        if (gwcPos >= 0) {
            geoserverUrl = geoserverUrl.substring(0, gwcPos);
        }

        String thumburl = geoserverUrl + "/wms/reflect?layers=ALA:" + layer.getName()
                + "&width=" + THUMBNAIL_WIDTH + "&height=" + THUMBNAIL_HEIGHT;

        InputStream is = null;
        ByteArrayOutputStream out = null;
        FileOutputStream fos = null;
        try {
            URL url = new URL(thumburl);

            is = url.openStream();
            InputStream in = new BufferedInputStream(is);
            out = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int n = 0;
            while (-1 != (n = in.read(buf))) {
                out.write(buf, 0, n);
            }

            String thumbnailFileName = thumbnailFileName(layer.getName(), thumbnailDir);

            FileUtils.deleteQuietly(new File(thumbnailFileName));

            fos = new FileOutputStream(thumbnailFileName);
            fos.write(out.toByteArray());
            fos.flush();
        } catch (Exception e) {
            logger.error("failed to create thumbnail for layer: " + layer.getName() + ", thumbnailDir: " + thumbnailDir, e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
