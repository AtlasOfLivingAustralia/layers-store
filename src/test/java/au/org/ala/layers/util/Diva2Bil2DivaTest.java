package au.org.ala.layers.util;

import au.org.ala.LayersStoreTest;
import au.org.ala.layers.intersect.Grid;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class Diva2Bil2DivaTest extends LayersStoreTest {
    // Test bil2diva and diva2bil are the same
    @Test
    public void conversionTest() {
        String tmpDir = System.getProperty("java.io.tmpdir");


        try {
            // create grid files (3)
            FileUtils.forceMkdir(new File(tmpDir + "/0.01"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el2.gri"), new File(tmpDir + "/0.01/el2.gri"));
            FileUtils.copyFile(new File(tmpDir + "/test-data/el2.grd"), new File(tmpDir + "/0.01/el2.grd"));

            Diva2bil.diva2bil(tmpDir + "/0.01/el1", tmpDir + "/0.01/el1");
            Bil2diva.bil2diva(tmpDir + "/0.01/el1", tmpDir + "/0.01/new", "units");

            Grid divaIn = new Grid(tmpDir + "/0.01/el1");
            Grid divaOut = new Grid(tmpDir + "/0.01/el1");

            assertTrue(new File(tmpDir + "/0.01/el1.bil").exists());
            assertTrue(new File(tmpDir + "/0.01/el1.hdr").exists());

            assertTrue(divaOut != null);
            assertTrue(divaOut.datatype.equals(divaIn.datatype));
            assertTrue(divaOut.minval == divaIn.minval);
            assertTrue(divaOut.maxval == divaIn.maxval);
            assertTrue(divaOut.xmin == divaIn.xmin);
            assertTrue(divaOut.ymax == divaIn.ymax);
            assertTrue(divaOut.byteorderLSB == divaIn.byteorderLSB);
            assertTrue(divaOut.ncols == divaIn.ncols);
            assertTrue(divaOut.nrows == divaIn.nrows);
            assertTrue(divaOut.nodatavalue == divaIn.nodatavalue);

        } catch (Exception e) {
            e.printStackTrace();

            assert (false);
        }
    }
}
