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

import static org.junit.Assert.assertTrue;

public class FieldDAOImplTest extends LayersStoreTest {

    @Before
    public void setup() {
        super.setup();

        String tmpDir = System.getProperty("java.io.tmpdir");

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
            layer1.setDisplaypath("displaypath");
            layer2.setId(2L);
            layer2.setType("Contextual");
            layer2.setKeywords("keyword2,keyword4");
            layer2.setName("2");
            layer2.setDisplayname("22");
            layer2.setDomain("terrestrial");
            layer2.setEnabled(true);
            layer2.setPath_orig("0.01");
            layer2.setDisplaypath("displaypath");
            layer3.setId(3L);
            layer3.setType("Contextual");
            layer3.setName("3");
            layer3.setDisplayname("33");
            layer3.setDomain("marine");
            layer3.setEnabled(false);
            layer3.setPath_orig("0.01");
            layer3.setDisplaypath("displaypath");

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
            fieldEl1.setIndb(true);
            fieldEl1.setDefaultlayer(true);
            fieldEl1.setEnabled(true);
            fieldEl2.setId("el2");
            fieldEl2.setName("2");
            fieldEl2.setSpid("2");
            fieldEl2.setIndb(false);
            fieldEl2.setDefaultlayer(true);
            fieldEl2.setEnabled(true);
            fieldEl3.setId("el3");
            fieldEl3.setName("3");
            fieldEl3.setSpid("3");
            fieldEl3.setIndb(true);
            fieldEl3.setDefaultlayer(true);
            fieldEl3.setEnabled(false);

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
        assertTrue(Client.getFieldDao().getFields().size() == 3);
        assertTrue(Client.getFieldDao().getFields(true).size() == 2);
        assertTrue(Client.getFieldDao().getFields(false).size() == 3);
    }

    // test get by id
    @Test
    public void testGetById() {
        assertTrue(Client.getFieldDao().getFieldById("el1").getId().equals("el1"));
        assertTrue(Client.getFieldDao().getFieldById("el3", true) == null);
        assertTrue(Client.getFieldDao().getFieldById("el3", false).getId().equals("el3"));
    }

    // test get by db
    @Test
    public void testGetInDb() {
        assertTrue(Client.getFieldDao().getFieldsByDB().size() == 1);
    }

    // test create/update/delete
    @Test
    public void testCreateUpdateDelete() {
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

        Field fieldEl4 = new Field();
        fieldEl4.setId("el4");
        fieldEl4.setName("4");
        fieldEl4.setSpid("4");
        fieldEl4.setSid("sid");
        fieldEl4.setSname("sname");
        fieldEl4.setSdesc("sdesc");
        fieldEl4.setType("e");

        fieldEl4.setDesc("desc");
        fieldEl4.setDefaultlayer(true);
        fieldEl4.setEnabled(true);
        fieldEl4.setIndb(true);
        fieldEl4.setAddtomap(true);
        fieldEl4.setAnalysis(true);
        fieldEl4.setIntersect(true);
        fieldEl4.setNamesearch(true);

        Client.getFieldDao().addField(fieldEl4);

        Field created = Client.getFieldDao().getFieldById("el4");
        assertTrue(fieldEl4.getId().equals(created.getId()));
        assertTrue(fieldEl4.getSpid().equals(created.getSpid()));
        assertTrue(fieldEl4.getType().equals(created.getType()));
        assertTrue(fieldEl4.getName().equals(created.getName()));
        assertTrue(fieldEl4.getSid().equals(created.getSid()));
        assertTrue(fieldEl4.getSname().equals(created.getSname()));
        assertTrue(fieldEl4.getDesc().equals(created.getDesc()));
        assertTrue(fieldEl4.getSdesc().equals(created.getSdesc()));
        assertTrue(fieldEl4.isAnalysis() == created.isAnalysis());
        assertTrue(fieldEl4.isDefaultlayer() == created.isDefaultlayer());
        assertTrue(fieldEl4.isEnabled() == created.isEnabled());
        assertTrue(fieldEl4.isIndb() == created.isIndb());
        assertTrue(fieldEl4.isIntersect() == created.isIntersect());
        assertTrue(fieldEl4.isAddtomap() == created.isAddtomap());

        fieldEl4.setDesc("desc2");
        fieldEl4.setDefaultlayer(false);
        fieldEl4.setEnabled(false);
        fieldEl4.setIndb(false);
        fieldEl4.setAddtomap(false);
        fieldEl4.setAnalysis(false);
        fieldEl4.setIntersect(false);
        fieldEl4.setNamesearch(false);

        Client.getFieldDao().updateField(fieldEl4);

        Field updated = Client.getFieldDao().getFieldById("el4", false);
        assertTrue(fieldEl4.getId().equals(updated.getId()));
        assertTrue(fieldEl4.getSpid().equals(updated.getSpid()));
        assertTrue(fieldEl4.getType().equals(updated.getType()));
        assertTrue(fieldEl4.getName().equals(updated.getName()));
        assertTrue(fieldEl4.getSid().equals(updated.getSid()));
        assertTrue(fieldEl4.getSname().equals(updated.getSname()));
        assertTrue(fieldEl4.getDesc().equals(updated.getDesc()));
        assertTrue(fieldEl4.getSdesc().equals(updated.getSdesc()));
        assertTrue(fieldEl4.isAnalysis() == updated.isAnalysis());
        assertTrue(fieldEl4.isDefaultlayer() == updated.isDefaultlayer());
        assertTrue(fieldEl4.isEnabled() == updated.isEnabled());
        assertTrue(fieldEl4.isIndb() == updated.isIndb());
        assertTrue(fieldEl4.isIntersect() == updated.isIntersect());
        assertTrue(fieldEl4.isAddtomap() == updated.isAddtomap());

        Client.getFieldDao().delete("el4");

        assertTrue(Client.getFieldDao().getFieldById("el4", false) == null);

    }

    // test search layers
    @Test
    public void testSearchLayers() {
        // domain search
        assertTrue(Client.getFieldDao().getLayersByCriteria("marine").size() == 1);
        // displayname search
        assertTrue(Client.getFieldDao().getLayersByCriteria("11").size() == 1);
        // name/displayname search
        assertTrue(Client.getFieldDao().getLayersByCriteria("1").size() == 1);
        // keywords search
        assertTrue(Client.getFieldDao().getLayersByCriteria("keyword1").size() == 1);
    }

    // test search fields
    @Test
    public void testSearchFields() {
        // domain search
        assertTrue(Client.getFieldDao().getFieldsByCriteria("marine").size() == 1);
        // displayname search
        assertTrue(Client.getFieldDao().getFieldsByCriteria("11").size() == 1);
        // name/displayname search
        assertTrue(Client.getFieldDao().getFieldsByCriteria("1").size() == 1);
        // keywords search
        assertTrue(Client.getFieldDao().getFieldsByCriteria("keyword1").size() == 1);
    }
}
