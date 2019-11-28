package au.org.ala.layers.client;

import au.org.ala.LayersStoreTest;
import au.org.ala.layers.dto.Field;
import au.org.ala.layers.dto.Layer;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ClientTest extends LayersStoreTest {

    @Test
    public void testClient() {

        try {
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

            for (Field f : Client.getFieldDao().getFields(false)) {
                Client.getFieldDao().delete(f.getId());
            }
            Client.getFieldDao().addField(fieldEl1);
            Client.getFieldDao().addField(fieldEl2);
            Client.getFieldDao().addField(fieldEl3);

        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue(Client.getLayerDao() != null);
        assertTrue(Client.getLayerIntersectDao() != null);
        assertTrue(Client.getFieldDao() != null);
        assertTrue(Client.getObjectDao() != null);
        assertTrue(Client.getUserDataDao() != null);

        assertTrue(Client.getLayerDao().getLayers().size() == 3);
        assertTrue(Client.getFieldDao().getFields().size() == 3);
    }

}
