/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package au.org.ala.layers.util;

import au.org.ala.LayersStoreTest;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Adam
 */
public class FixGridMinMaxTest extends LayersStoreTest {

    @Before
    public void setup() {
        super.setup();
    }

    // Test that the correct min/max are returned for a grid file
    @Test
    public void testFixMinMax() {
        String tmpDir = System.getProperty("java.io.tmpdir");

        try {
            FileUtils.forceMkdir(new File(tmpDir + "/0.01"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el1.gri"), new File(tmpDir + "/headertest/el1.gri"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el1.grd"), new File(tmpDir + "/headertest/el1.grd"));

            FileUtils.forceMkdir(new File(tmpDir + "/fixedheaders"));

            String header = FileUtils.readFileToString(new File(tmpDir + "/headertest/el1.grd"));
            FileUtils.writeStringToFile(new File(tmpDir + "/headertest/el1.grd"), header.replace("MinValue=-43", "MinValue=0").replace("MaxValue=3492", "MaxValue=0"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        FixGridMinMax.main(new String[]{tmpDir + "/headertest", tmpDir + "/fixedheaders/"});

        String header = "";
        try {
            header = FileUtils.readFileToString(new File(tmpDir + "/fixedheaders/el1.grd"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue(header.contains("MinValue=-43"));
        assertTrue(header.contains("MaxValue=3492"));

        assertFalse(header.contains("MinValue=0"));
        assertFalse(header.contains("MaxValue=0"));
    }

}
