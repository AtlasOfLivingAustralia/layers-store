/**************************************************************************
 * Copyright (C) 2012 Atlas of Living Australia
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
import org.junit.Before;

public class DistributionDAOImplTest extends LayersStoreTest {

    @Before
    public void setup() {
        super.setup();

        try {

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

            jdbcTemplate.execute("DELETE from distributions;");

            String data_resource_uid = "dr804";
            int geom_idx = 5;
            String wmsurl = "wmsurl";
            String area_name = "Expert distribution 1";
            double area_km = 6196870.041770578;
            int spcode = 1;
            int gid = 25056;
            String lsid = "1";
            String scientific = "Enicurus leschenaulti";
            String genus_name = "Enicurus";
            String specific_n = "leschenaulti";
            String group_name = "";
            boolean endemic = false;
            String imageUrl = "imageUrl";
            String type = "e";
            String common_nam = "";
            String family_lsid = "";
            String genus_lsid = "";
            String family = "";

            String insertSql = "INSERT INTO distributions (geom_idx, wmsurl, area_name, area_km, spcode, gi, lsid, " +
                    "scientific, genus_name, specific_n, group_name, endemic, image_url, type) VALUES (" +
                    "'" + data_resource_uid + "'," + geom_idx + ",'" + wmsurl + "','" + area_name + "'," + area_km +
                    "," + spcode + "," + gid + ",'" + lsid + "','" + scientific + "','" + genus_name + "'," + endemic +
                    ",'" + imageUrl + "','" + type + "','" + common_nam + "','" + family_lsid + "','" + genus_lsid + "','" + family + "');";
            jdbcTemplate.execute(insertSql);

            data_resource_uid = "dr804";
            geom_idx = 6;
            wmsurl = "wmsurl";
            area_name = "Expert distribution 2";
            area_km = 1080679.7397204211;
            spcode = 2;
            gid = 25084;
            lsid = "2";
            scientific = "Erythrotriorchis radiatus";
            genus_name = "Enicurus";
            specific_n = "leschenaulti";
            group_name = "";
            endemic = false;
            imageUrl = "imageUrl";
            type = "e";
            common_nam = "Red Goshawk";
            family_lsid = "urn:lsid:biodiversity.org.au:afd.taxon:6257e334-274c-4439-bbf2-7cd404803498";
            genus_lsid = "urn:lsid:biodiversity.org.au:afd.taxon:bbfbd490-101c-45a3-a1fa-5d639dea52bd";
            family = "ACCIPITRIDAE";

            insertSql = "INSERT INTO distributions (geom_idx, wmsurl, area_name, area_km, spcode, gi, lsid, " +
                    "scientific, genus_name, specific_n, group_name, endemic, image_url, type) VALUES (" +
                    "'" + data_resource_uid + "'," + geom_idx + ",'" + wmsurl + "','" + area_name + "'," + area_km +
                    "," + spcode + "," + gid + ",'" + lsid + "','" + scientific + "','" + genus_name + "'," + endemic +
                    ",'" + imageUrl + "','" + type + "','" + common_nam + "','" + family_lsid + "','" + genus_lsid + "','" + family + "');";
            jdbcTemplate.execute(insertSql);

            data_resource_uid = "dr804";
            geom_idx = 7;
            wmsurl = "wmsurl";
            area_name = "Expert distribution 3";
            area_km = 10;
            spcode = 3;
            gid = 3;
            lsid = "3";
            scientific = "Eudyptes chrysocome";
            genus_name = "Eudyptes";
            specific_n = "chrysocome";
            group_name = "";
            endemic = false;
            imageUrl = "imageUrl";
            type = "e";
            common_nam = "Rockhopper Penguin\"";
            family_lsid = "NZOR-3-8719";
            genus_lsid = "urn:lsid:biodiversity.org.au:afd.taxon:9d438a3d-4fd6-40fd-84e9-a40fbd17f9ca";
            family = "SPHENISCIDAE";

            insertSql = "INSERT INTO distributions (geom_idx, wmsurl, area_name, area_km, spcode, gi, lsid, " +
                    "scientific, genus_name, specific_n, group_name, endemic, image_url, type) VALUES (" +
                    "'" + data_resource_uid + "'," + geom_idx + ",'" + wmsurl + "','" + area_name + "'," + area_km +
                    "," + spcode + "," + gid + ",'" + lsid + "','" + scientific + "','" + genus_name + "'," + endemic +
                    ",'" + imageUrl + "','" + type + "','" + common_nam + "','" + family_lsid + "','" + genus_lsid + "','" + family + "');";
            jdbcTemplate.execute(insertSql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // test find by lsid or name

    // test find list by lsid or name

    // test query

    // test query counts

    // test get by spcode

    // test query by radius

    // test query counts by radius

    // test get by lsids

    // test query vertices count

    // test save

    // test outlier identification
}
