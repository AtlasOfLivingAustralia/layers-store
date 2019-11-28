package au.org.ala.spatial.analysis.layers;

import au.org.ala.LayersStoreTest;
import au.org.ala.layers.intersect.SimpleRegion;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class RecordsTest extends LayersStoreTest {

    @Test
    public void testGet() {
        try {
            SimpleRegion region = null;
            File tmpFile = File.createTempFile("records", ".csv");
            String facetName = "names_and_lsids";
            String q = "*:*";
            String biocache_service_url = "http://ws";
            double minx = -180;
            double maxx = 180;
            double miny = -90;
            double maxy = 90;

            Records r = new RecordsWithMockedStaticMethod(biocache_service_url, q, new double[]{minx, miny, maxx, maxy}, tmpFile.getPath(), region, facetName);

            assert (RecordsWithMockedStaticMethod.urlRequests.size() == 2);

            String url1 = biocache_service_url + "/webportal/occurrences.gz?q=" + q + "&fq=longitude:%5B" + minx + "%20TO%20" + maxx + "%5D&fq=latitude:%5B" + miny + "%20TO%20" + maxy + "%5D&pageSize=50000&start=0&fl=longitude,latitude," + facetName + ",year";
            String url2 = biocache_service_url + "/webportal/occurrences.gz?q=" + q + "&fq=longitude:%5B" + minx + "%20TO%20" + maxx + "%5D&fq=latitude:%5B" + miny + "%20TO%20" + maxy + "%5D&pageSize=50000&start=50000&fl=longitude,latitude," + facetName + ",year";

            assert (url1.equals(RecordsWithMockedStaticMethod.urlRequests.get(0)));
            assert (url2.equals(RecordsWithMockedStaticMethod.urlRequests.get(1)));

            assert (r.getSpeciesSize() == 1);
            assert (r.getRecordsSize() == 50001);


            // test saved copy
            Records r2 = new Records(tmpFile.getPath());
            assert (r2.getSpeciesSize() == 1);
            assert (r2.getRecordsSize() == 50001);
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }

    @Test
    public void testLoad() {
        String filename = System.getProperty("java.io.tmpdir") + "/test-data/records.csv";

        Records r = null;
        try {
            r = new Records(filename);
        } catch (Exception e) {

        }

        assert (r != null);
        assert (r.getRecordsSize() == 10);
        assert (r.getSpeciesSize() == 2);
    }

    @Test
    public void testLoadWithinAnArea() {
        String filename = System.getProperty("java.io.tmpdir") + "/test-data/records.csv";

        SimpleRegion area = SimpleRegion.parseSimpleRegion("POLYGON((145 -10,170 -10,170 -50,145 -50,145 -10))");

        Records r = null;
        try {
            r = new Records(filename, area);
        } catch (Exception e) {

        }

        assert (r != null);
        assert (r.getRecordsSize() == 4);
        assert (r.getSpeciesSize() == 1);
    }

    @Test
    public void testSorting() {
        String filename = "/data/records.csv";

        Records r = null;
        try {
            r = new Records(filename);
        } catch (Exception e) {

        }

        r.sortedStarts(-90, -180, 0.1);

        double lastlng = r.getSortedLongitude(0);
        double lastlat = r.getSortedLatitude(0);
        for (int i = 1; i < r.getRecordsSize(); i++) {
            double lng = r.getSortedLongitude(i);
            double lat = r.getSortedLatitude(i);
            assert (lastlat <= lat || lastlng <= lng);
            lastlng = lng;
        }

        for (int i = 0; i < r.getSpeciesSize(); i++) {
            double species = r.getSortedSpeciesNumber(i);
        }
    }
}

class RecordsWithMockedStaticMethod extends Records {

    static List<String> urlRequests = new ArrayList();

    InputStream getUrlStream(String url) throws IOException {
        urlRequests.add(url);

        InputStream stream = new InputStream() {
            int row = 0;
            int pos = 0;

            String header = "names_and_lsids,longitude,latitude,year\n";
            String record = "a,1,2,2000,0\n";

            @Override
            public int read() throws IOException {
                // stream two pages of records
                // first page record length is 50000+1 (+1 for header)
                // second page record length is 1+1 (+1 for header)

                if (row == 0) {
                    char c = header.charAt(pos++);

                    if (c == '\n') {
                        row++;
                        pos = 0;
                    }

                    return c;
                } else if ((urlRequests.size() == 1 && row < 50001) || (urlRequests.size() == 2 && row < 2)) {
                    char c = record.charAt(pos++);

                    if (c == '\n') {
                        row++;
                        pos = 0;
                    }

                    return c;
                }

                return -1;
            }
        };

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream os = new GZIPOutputStream(baos);
        IOUtils.copy(stream, os);
        os.finish();

        InputStream is = new ByteArrayInputStream(baos.toByteArray());

        return is;
    }

    public RecordsWithMockedStaticMethod(String biocache_service_url, String q, double[] bbox, String filename, SimpleRegion region) throws IOException {
        super(biocache_service_url, q, bbox, filename, region);
    }

    public RecordsWithMockedStaticMethod(String biocache_service_url, String q, double[] bbox, String filename, SimpleRegion region, String facetField) throws IOException {
        super(biocache_service_url, q, bbox, filename, region, facetField);
    }
}
