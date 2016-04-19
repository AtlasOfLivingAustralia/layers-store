package au.org.ala.spatial.analysis.layers;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.layers.intersect.SimpleRegion;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.util.ByteArrayBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author Adam
 */
public class LayersServiceRecords extends Records {

    private static final Logger logger = Logger.getLogger(LayersServiceRecords.class);

    public LayersServiceRecords(String biocache_service_url, String q, double[] bbox, String filename, SimpleRegion region) throws IOException {
        super(biocache_service_url, q, bbox, filename, region);
    }

    public LayersServiceRecords(String biocache_service_url, String q, double[] bbox, String filename, SimpleRegion region, String facetField) throws IOException {
        super(biocache_service_url, q, bbox, filename, region, facetField);
    }

    @Override
    void init(String biocache_service_url, String q, double[] bbox, String filename, SimpleRegion region, String facetField) throws IOException {
        int speciesEstimate = 250000;
        int recordsEstimate = 26000000;
        int pageSize = 50000;

        if (bbox == null) {
            bbox = new double[]{-180, -90, 180, 90};
        }

        String bboxTerm = String.format("longitude:%%5B%f%%20TO%%20%f%%5D%%20AND%%20latitude:%%5B%f%%20TO%%20%f%%5D", bbox[0], bbox[2], bbox[1], bbox[3]);

        points = new ArrayList<Double>(recordsEstimate);
        lsidIdx = new ArrayList<Integer>(recordsEstimate);
        years = new ArrayList<Short>(recordsEstimate);
        HashMap<String, Integer> lsidMap = new HashMap<String, Integer>(speciesEstimate);

        int start = 0;

        RandomAccessFile raf = null;

        try {
            if (filename != null) {
                raf = new RandomAccessFile(filename, "rw");
            }

            while (true && start < 300000000) {
                String url = biocache_service_url + "/userdata/sample?q=" + q;

                logger.info("url: " + url);

                CSVReader csv = null;
                InputStream is = null;
                try {
                    is = getUrlStream(url);
                    ZipInputStream zis = new ZipInputStream(is);
                    ZipEntry ze = zis.getNextEntry();

                    ByteArrayBuilder bab = new ByteArrayBuilder();
                    byte[] b = new byte[1024];
                    int n;
                    while ((n = zis.read(b, 0, 1024)) > 0) {
                        bab.write(b, 0, n);
                    }

                    csv = new CSVReader(new StringReader(IOUtils.toString(bab.toByteArray(), "UTF-8")));


                    if (csv == null) {
                        throw new IOException("failed to get records from layers-service.");
                    }

                    String[] line;
                    int[] header = new int[2]; //to contain [0]=longitude, [1]=latitude
                    int row = start;
                    int currentCount = 0;
                    while ((line = csv.readNext()) != null) {
                        if (raf != null) {
                            for (int i = 0; i < line.length; i++) {
                                if (i > 0) {
                                    raf.write(",".getBytes());
                                }
                                raf.write(line[i].getBytes());
                            }
                            raf.write("\n".getBytes());
                        }
                        currentCount++;
                        if (currentCount == 1) {
                            //determine header
                            for (int i = 0; i < line.length; i++) {
                                if (line[i].equals("longitude")) {
                                    header[0] = i;
                                }
                                if (line[i].equals("latitude")) {
                                    header[1] = i;
                                }
                            }
                            logger.info("header info:" + header[0] + "," + header[1]);
                        } else {
                            if (line.length >= 3) {
                                try {
                                    double longitude = Double.parseDouble(line[header[0]]);
                                    double latitude = Double.parseDouble(line[header[1]]);
                                    if (region == null || region.isWithin_EPSG900913(longitude, latitude)) {
                                        points.add(longitude);
                                        points.add(latitude);
                                        String species = "default";
                                        Integer idx = lsidMap.get(species);
                                        if (idx == null) {
                                            idx = lsidMap.size();
                                            lsidMap.put(species, idx);
                                        }
                                        lsidIdx.add(idx);
                                        years.add((short) 0);   //default
                                    }
                                } catch (Exception e) {
                                } finally {
                                    if (lsidIdx.size() * 2 < points.size()) {
                                        points.remove(points.size() - 1);
                                        points.remove(points.size() - 1);
                                    } else if (years.size() < lsidIdx.size()) {
                                        years.add((short) 0);
                                    }
                                }
                            }
                        }
                        row++;
                    }
                    if (start == 0) {
                        start = row - 1; //offset for header
                    } else {
                        start = row;
                    }

                    if (currentCount == 0 || currentCount < pageSize) {
                        break;
                    }
                } catch (Exception e) {
                    logger.error("failed to get userdata as csv for url: " + url, e);
                } finally {
                    if (is != null) {
                        try {
                            is.close();
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    if (csv != null) {
                        try {
                            csv.close();
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        //make lsid list
        lsids = new String[lsidMap.size()];
        for (Entry<String, Integer> e : lsidMap.entrySet()) {
            lsids[e.getValue()] = e.getKey();
        }

        logger.info("Got " + getRecordsSize() + " records of " + getSpeciesSize() + " species");
    }
}
