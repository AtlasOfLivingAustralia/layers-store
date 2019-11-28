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

public class TabulationDAOImplTest extends LayersStoreTest {

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
            layer1.setId(5L);
            layer1.setType("Contextual");
            layer1.setKeywords("keyword1,keyword2,keyword4");
            layer1.setName("5");
            layer1.setDisplayname("55");
            layer1.setDomain("marine,terrestrial");
            layer1.setEnabled(true);
            layer1.setPath_orig("0.01");
            layer1.setDisplaypath("displaypath");
            layer2.setId(6L);
            layer2.setType("Contextual");
            layer2.setKeywords("keyword2,keyword4");
            layer2.setName("6");
            layer2.setDisplayname("66");
            layer2.setDomain("terrestrial");
            layer2.setEnabled(true);
            layer2.setPath_orig("0.01");
            layer2.setDisplaypath("displaypath");
            layer3.setId(7L);
            layer3.setType("Contextual");
            layer3.setName("7");
            layer3.setDisplayname("77");
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
            fieldEl1.setId("cl5");
            fieldEl1.setType("c");
            fieldEl1.setName("5");
            fieldEl1.setSpid("5");
            fieldEl1.setIndb(true);
            fieldEl1.setDefaultlayer(true);
            fieldEl1.setEnabled(true);
            fieldEl2.setId("cl6");
            fieldEl2.setType("c");
            fieldEl2.setName("6");
            fieldEl2.setSpid("6");
            fieldEl2.setIndb(false);
            fieldEl2.setDefaultlayer(true);
            fieldEl2.setEnabled(true);
            fieldEl3.setId("cl7");
            fieldEl3.setType("c");
            fieldEl3.setName("7");
            fieldEl3.setSpid("7");
            fieldEl3.setIndb(true);
            fieldEl3.setDefaultlayer(true);
            fieldEl3.setEnabled(false);

            for (Field f : Client.getFieldDao().getFields(false)) {
                Client.getFieldDao().delete(f.getId());
            }
            Client.getFieldDao().addField(fieldEl1);
            Client.getFieldDao().addField(fieldEl2);
            Client.getFieldDao().addField(fieldEl3);

            String fid1 = "cl5";
            String fid2 = "cl6";
            String fid3 = "cl7";
            String pid1 = "5";
            String pid2 = "6";
            String pid3 = "7";
            double area = 1.5;
            int occurrences = 2;
            int species = 3;
            String polygon = "POLYGON ((131 -22,131 -20,133 -20,133 -22,131 -22))";
            String geom = "st_setsrid(st_geomfromtext('" + polygon + "'), 4326)";

            jdbcTemplate.execute("DELETE from tabulation;");
            jdbcTemplate.execute("DELETE from objects;");

            String insertSql = "INSERT INTO tabulation (fid1, fid2, pid1, pid2, area, occurrences, species, the_geom) VALUES " +
                    "('" + fid1 + "','" + fid2 + "'," + "'" + pid1 + "','" + pid2 + "'," + area + "," + occurrences +
                    "," + species + "," + geom + ");";
            jdbcTemplate.execute(insertSql);

            insertSql = "INSERT INTO tabulation (fid1, fid2, pid1, pid2, area, occurrences, species, the_geom) VALUES " +
                    "('" + fid1 + "','" + fid3 + "'," + "'" + pid1 + "','" + pid3 + "'," + area + "," + occurrences +
                    "," + species + "," + geom + ");";
            jdbcTemplate.execute(insertSql);

            insertSql = "INSERT INTO tabulation (fid1, fid2, pid1, pid2, area, occurrences, species, the_geom) VALUES " +
                    "('" + fid2 + "','" + fid3 + "'," + "'" + pid2 + "','" + pid3 + "'," + area + "," + occurrences +
                    "," + species + "," + geom + ");";
            jdbcTemplate.execute(insertSql);

            insertSql = "INSERT INTO objects (fid, pid, id, name, area_km, namesearch, the_geom, bbox) VALUES " +
                    "('" + fid1 + "','" + pid1 + "'," + "'" + pid1 + "','" + pid1 + "'," + area + ",true," +
                    "st_setsrid(st_geomfromtext('" + polygon + "'),4326), '" + polygon + "');";
            jdbcTemplate.execute(insertSql);

            insertSql = "INSERT INTO objects (fid, pid, id, name, area_km, namesearch, the_geom, bbox) VALUES " +
                    "('" + fid2 + "','" + pid2 + "'," + "'" + pid2 + "','" + pid2 + "'," + area + ",true," +
                    "st_setsrid(st_geomfromtext('" + polygon + "'),4326), '" + polygon + "' );";
            jdbcTemplate.execute(insertSql);

            insertSql = "INSERT INTO objects (fid, pid, id, name, area_km, namesearch, the_geom, bbox) VALUES " +
                    "('" + fid3 + "','" + pid3 + "'," + "'" + pid3 + "','" + pid3 + "'," + area + ",true," +
                    "st_setsrid(st_geomfromtext('" + polygon + "'),4326), '" + polygon + "' );";
            jdbcTemplate.execute(insertSql);

            Client.getLayerIntersectDao().getConfig().load();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // test get by fid1, fid2, wkt
    @Test
    public void testGetByFid1Fid2Wkt() {
        String fid1 = "cl5";
        String fid2 = "cl6";
        String wkt = null;
        String wktInside = "POLYGON ((131 -22,131 -20,133 -20,133 -22,131 -22))";
        String wktOutside = "POLYGON ((134 -22,134 -20,135 -20,135 -22,134 -22))";

        assertTrue(Client.getTabulationDao().getTabulation(fid1, fid2, wkt).size() == 1);
        assertTrue(Client.getTabulationDao().getTabulation(fid1, fid2, wktInside).size() == 1);
        assertTrue(Client.getTabulationDao().getTabulation(fid1, fid2, wktOutside).size() == 0);

        // species and occurrence counts are the intersection of wktInside, tabulation geometry and test-data/records.csv
        assertTrue(Client.getTabulationDao().getTabulation(fid1, fid2, wktInside).get(0).getSpecies() == 2);
        assertTrue(Client.getTabulationDao().getTabulation(fid1, fid2, wktInside).get(0).getOccurrences() == 2);
    }

    // test list all
    @Test
    public void testGetTabulationSingle() {
        String fid1 = "cl5";
        String wktInside = "POLYGON ((131 -22,131 -20,133 -20,133 -22,131 -22))";
        String wktOutside = "POLYGON ((134 -22,134 -20,135 -20,135 -22,134 -22))";

        assertTrue(Client.getTabulationDao().getTabulationSingle(fid1, wktInside).size() == 1);
        assertTrue(Client.getTabulationDao().getTabulationSingle(fid1, wktOutside).size() == 0);
    }
}
