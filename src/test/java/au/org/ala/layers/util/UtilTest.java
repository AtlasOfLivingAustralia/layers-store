package au.org.ala.layers.util;

import au.org.ala.LayersStoreTest;
import au.org.ala.layers.dto.Layer;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class UtilTest extends LayersStoreTest {

    // test URL read

    // test updateDisplayPaths
    @Test
    public void testUpdateDisplayPaths() {
        Layer layer = new Layer();
        layer.setName("test");
        layer.setDisplaypath("/path");

        Util.updateDisplayPaths(Arrays.asList(layer));

        assertTrue("http://localhost:8078/geoserver/path".equals(layer.getDisplaypath()));

        layer = new Layer();
        layer.setName("test");
        layer.setDisplaypath("<COMMON_GEOSERVER_URL>/path");

        Util.updateDisplayPaths(Arrays.asList(layer));

        assertTrue("http://localhost:8078/geoserver/path".equals(layer.getDisplaypath()));
    }

    // test updateMetadataPaths
    @Test
    public void testUpdateMetadataPaths() {
        Layer layer = new Layer();
        layer.setName("test");
        layer.setMetadatapath("/path");

        Util.updateMetadataPaths(Arrays.asList(layer));

        assertTrue("http://localhost:8079/geonetwork/path".equals(layer.getMetadatapath()));

        layer = new Layer();
        layer.setName("test");
        layer.setMetadatapath("<COMMON_GEONETWORK_URL>/path");

        Util.updateMetadataPaths(Arrays.asList(layer));

        assertTrue("http://localhost:8079/geonetwork/path".equals(layer.getMetadatapath()));
    }
}
