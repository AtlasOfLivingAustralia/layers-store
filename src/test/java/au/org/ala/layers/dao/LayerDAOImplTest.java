/**************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia
 * All Rights Reserved.
 * <p>
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * <p>
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.layers.dao;

import au.org.ala.LayersStoreTest;
import au.org.ala.layers.client.Client;
import au.org.ala.layers.dto.Field;
import au.org.ala.layers.dto.Layer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LayerDAOImplTest extends LayersStoreTest {

    @Before
    public void setup() {
        super.setup();

        try {
            for (Layer l : Client.getLayerDao().getLayersForAdmin()) {
                Client.getLayerDao().delete("" + l.getId());
            }

            Layer layer1 = new Layer();
            Layer layer2 = new Layer();
            Layer layer3 = new Layer();
            layer1.setId(1L);
            layer1.setType("Environmental");
            layer1.setKeywords("keyword1,keyword2,keyword4");
            layer1.setName("1");
            layer1.setDisplayname("11");
            layer1.setDomain("marine,terrestrial");
            layer1.setEnabled(true);
            layer1.setPath_orig("0.01");
            layer2.setId(2L);
            layer2.setType("Contextual");
            layer2.setKeywords("keyword2,keyword4");
            layer2.setName("2");
            layer2.setDisplayname("22");
            layer2.setDomain("terrestrial");
            layer2.setEnabled(true);
            layer2.setPath_orig("0.01");
            layer3.setId(3L);
            layer3.setType("Contextual");
            layer3.setName("3");
            layer3.setDisplayname("33");
            layer3.setDomain("marine");
            layer3.setEnabled(false);
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
    }

    // test get all
    @Test
    public void testGetAll() {
        assertTrue(Client.getLayerDao().getLayers().size() == 2);
        assertTrue(Client.getLayerDao().getLayersForAdmin().size() == 3);
    }

    // test get by id
    @Test
    public void testGetById() {
        assertTrue(Client.getLayerDao().getLayerById(1).getId() == 1);
        assertTrue(Client.getLayerDao().getLayerById(3, true) == null);
        assertTrue(Client.getLayerDao().getLayerById(3, false).getId() == 3);
    }

    // test get by name
    @Test
    public void testGetByName() {
        assertTrue(Client.getLayerDao().getLayerByName("1").getId() == 1);
        assertTrue(Client.getLayerDao().getLayerByName("3", true) == null);
        assertTrue(Client.getLayerDao().getLayerByName("3", false).getId() == 3);
    }

    // test get by display name
    @Test
    public void testGetByDisplayName() {
        assertTrue(Client.getLayerDao().getLayerByDisplayName("11").getId() == 1);
        assertTrue(Client.getLayerDao().getLayerByDisplayName("33") == null);
    }

    // test get environmental
    @Test
    public void testGetEnvironmental() {
        assertTrue(Client.getLayerDao().getLayersByEnvironment().size() == 1);
    }

    // test get contextual
    @Test
    public void testGetContextual() {
        assertTrue(Client.getLayerDao().getLayersByContextual().size() == 1);
    }

    // test get by criteria
    @Test
    public void testGetByCriteria() {
        assertTrue(Client.getLayerDao().getLayersByCriteria("keyword1").size() == 1);
        assertTrue(Client.getLayerDao().getLayersByCriteria("keyword2").size() == 2);
        assertTrue(Client.getLayerDao().getLayersByCriteria("keyword3").size() == 0);
    }

    @Test
    public void testUpdate() {
        String notes = "notes";

        Layer layer = Client.getLayerDao().getLayerById(1);

        assertFalse(notes.equals(layer.getNotes()));

        layer.setNotes(notes);

        Client.getLayerDao().updateLayer(layer);

        Layer updatedLayer = Client.getLayerDao().getLayerById(1);

        assertTrue(notes.equals(updatedLayer.getNotes()));
    }

    @Test
    public void testCreateDelete() {
        Layer layer4 = new Layer();
        layer4.setId(4L);
        layer4.setType("Environmental");
        layer4.setKeywords("keyword1,keyword2,keyword4");
        layer4.setName("4");
        layer4.setDisplayname("44");
        layer4.setDomain("marine,terrestrial");
        layer4.setEnabled(true);
        layer4.setPath_orig("0.01");

        Client.getLayerDao().addLayer(layer4);

        Layer createdLayer = Client.getLayerDao().getLayerById(4);
        assertTrue(createdLayer != null);
        assertTrue(layer4.getType().equals(createdLayer.getType()));
        assertTrue(layer4.getName().equals(createdLayer.getName()));
        assertTrue(layer4.getDisplayname().equals(createdLayer.getDisplayname()));
        assertTrue(layer4.getdomain().equals(createdLayer.getdomain()));
        assertTrue(layer4.getKeywords().equals(createdLayer.getKeywords()));
        assertTrue(layer4.getPath_orig().equals(createdLayer.getPath_orig()));
        assertTrue(layer4.getEnabled() == createdLayer.getEnabled());

        Client.getLayerDao().delete("4");

        assertTrue(Client.getLayerDao().getLayerById(4) == null);
    }
}
