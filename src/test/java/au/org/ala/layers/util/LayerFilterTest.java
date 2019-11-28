package au.org.ala.layers.util;

import au.org.ala.LayersStoreTest;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * container for layer filter;
 * <p/>
 * - includes minimum and maximum for environmental layers
 * - includes catagory names and indexes of selected for contextual layers
 *
 * @author adam
 */
public class LayerFilterTest extends LayersStoreTest {

    // test parsing
    @Test
    public void testParsing() {
        String raw1 = "el1,1,2:el2,0,5";
        String raw2 = "ENVELOPE(el1,1,2:el2,0,5)";
        String raw3 = "ENVELOPE(cl4,1,2,3)";

        LayerFilter[] parsed1 = LayerFilter.parseLayerFilters(raw1);
        LayerFilter[] parsed2 = LayerFilter.parseLayerFilters(raw2);
        LayerFilter[] parsed3 = LayerFilter.parseLayerFilters(raw3);

        assertTrue(parsed1[0].getLayername().equals("el1"));
        assertTrue(parsed1[0].isValid(1.5));
        assertFalse(parsed1[0].isValid(3));
        assertFalse(parsed1[0].isContextual());

        assertTrue(parsed1[1].getLayername().equals("el2"));
        assertTrue(parsed1[1].isValid(3));
        assertFalse(parsed1[1].isValid(8));
        assertFalse(parsed1[1].isContextual());

        assertTrue(parsed2[0].getLayername().equals("el1"));
        assertTrue(parsed2[0].isValid(1.5));
        assertFalse(parsed2[0].isValid(3));
        assertFalse(parsed2[0].isContextual());

        assertTrue(parsed2[1].getLayername().equals("el2"));
        assertTrue(parsed2[1].isValid(3));
        assertFalse(parsed2[1].isValid(8));
        assertFalse(parsed2[1].isContextual());

        assertTrue(parsed3[0].getLayername().equals("cl4"));
        assertTrue(parsed3[0].isContextual());
        assertTrue(parsed3[0].getIds().length == 3);
        assertTrue(parsed3[0].getIds()[0].equals("1"));
        assertTrue(parsed3[0].getIds()[1].equals("2"));
        assertTrue(parsed3[0].getIds()[2].equals("3"));
    }

}
