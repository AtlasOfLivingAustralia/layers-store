package au.org.ala.spatial.analysis.layers;

import au.org.ala.LayersStoreTest;
import au.org.ala.layers.client.Client;
import au.org.ala.layers.dto.Field;
import au.org.ala.layers.dto.Layer;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LayerDistanceIndexTest extends LayersStoreTest {

    @Before
    public void setup() {
        super.setup();

    }

    // test get distances
    @Test
    public void testGetLayerDistanceIndex() {
        String tmpDir = System.getProperty("java.io.tmpdir");

        // create layer distances file
        try {
            FileUtils.writeStringToFile(new File(tmpDir + "/layerDistances.properties"), "el1 el2=0.5\nel2 el3=0.2\nel1 el3=0.4\n");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, Double> distances = LayerDistanceIndex.loadDistances();

        assertTrue(distances.size() == 3);
        assertTrue(distances.get("el1 el2") == 0.5);
        assertTrue(distances.get("el2 el3") == 0.2);
        assertTrue(distances.get("el1 el3") == 0.4);

    }

    // applies correct domain vs domain restrictions (LayerDistanceIndex)
    @Test
    public void testDomainCheck() {
        String[] domains1 = LayerDistanceIndex.parseDomain("marine,terrestrial");
        String[] domains2 = LayerDistanceIndex.parseDomain("terrestrial");
        String[] domains3 = LayerDistanceIndex.parseDomain("marine");

        assertTrue(LayerDistanceIndex.isSameDomain(domains1, domains2));
        assertTrue(LayerDistanceIndex.isSameDomain(domains1, domains3));
        assertFalse(LayerDistanceIndex.isSameDomain(domains2, domains3));
    }

    // test creation of missing distances
    @Test
    public void testCreationOfDistances() {
        String tmpDir = System.getProperty("java.io.tmpdir");

        try {
            // create layer distances file
            FileUtils.writeStringToFile(new File(tmpDir + "/layerDistances.properties"), "el1 el2=0.5\n");

            // create grid files (3)
            FileUtils.forceMkdir(new File(tmpDir + "/0.01"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el1.gri"), new File(tmpDir + "/0.01/el1.gri"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el1.grd"), new File(tmpDir + "/0.01/el1.grd"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el2.gri"), new File(tmpDir + "/0.01/el2.gri"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el2.grd"), new File(tmpDir + "/0.01/el2.grd"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el2.gri"), new File(tmpDir + "/0.01/el3.gri"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el2.grd"), new File(tmpDir + "/0.01/el3.grd"));

            for (Layer l : Client.getLayerDao().getLayersForAdmin()) {
                Client.getLayerDao().delete("" + l.getId());
            }

            Layer layer1 = new Layer();
            Layer layer2 = new Layer();
            Layer layer3 = new Layer();
            layer1.setId(1L);
            layer1.setType("Environmental");
            layer1.setName("1");
            layer1.setDomain("marine,terrestrial");
            layer1.setEnabled(true);
            layer1.setPath_orig("0.01");
            layer2.setId(2L);
            layer2.setType("Environmental");
            layer2.setName("2");
            layer2.setDomain("terrestrial");
            layer2.setEnabled(true);
            layer2.setPath_orig("0.01");
            layer3.setId(3L);
            layer3.setType("Environmental");
            layer3.setName("3");
            layer3.setDomain("marine");
            layer3.setEnabled(true);
            layer3.setPath_orig("0.01");

            Client.getLayerDao().addLayer(layer1);
            Client.getLayerDao().addLayer(layer2);
            Client.getLayerDao().addLayer(layer3);
            Client.getLayerDao().updateLayer(layer1);
            Client.getLayerDao().updateLayer(layer2);
            Client.getLayerDao().updateLayer(layer3);

            Field fieldEl1 = new Field();
            Field fieldEl2 = new Field();
            Field fieldEl3 = new Field();
            fieldEl1.setId("el1");
            fieldEl1.setName("1");
            fieldEl1.setSpid("1");
            fieldEl1.setDefaultlayer(true);
            fieldEl1.setEnabled(true);
            fieldEl2.setId("el2");
            fieldEl2.setName("2");
            fieldEl2.setSpid("2");
            fieldEl2.setDefaultlayer(true);
            fieldEl2.setEnabled(true);
            fieldEl3.setId("el3");
            fieldEl3.setName("3");
            fieldEl3.setSpid("3");
            fieldEl3.setDefaultlayer(true);
            fieldEl3.setEnabled(true);

            for (Field f : Client.getFieldDao().getFields()) {
                Client.getFieldDao().delete(f.getId());
            }
            Client.getFieldDao().addField(fieldEl1);
            Client.getFieldDao().addField(fieldEl2);
            Client.getFieldDao().addField(fieldEl3);

            Client.getLayerIntersectDao().getConfig().load();
        } catch (Exception e) {
            e.printStackTrace();
        }

        LayerDistanceIndex.all();

        Map<String, Double> distances = LayerDistanceIndex.loadDistances();

        assertTrue(distances.size() == 2);
        assertTrue(distances.get("el1 el2") == 0.5); // unchanged
        assertFalse(distances.containsKey("el2 el3")); // missing because domains do not match
        assertTrue(distances.get("el1 el3") == 0.08062008371842853); // new value
    }
}
