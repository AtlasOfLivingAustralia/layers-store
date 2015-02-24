package au.org.ala.layers.tabulation;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.GeometryBuilder;
import org.geotools.graph.util.ZipUtil;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by a on 3/02/15.
 */
public class Intersection {

    public static void main(String[] args) {
        System.out.println("produces a shapefile with the intersection of all.\r\n" +
                "usage: shapefile1 shapefile2 outputShapefile");

        File f = new File("/data/ala/data/layers/ready/shape/");
        File[] list = f.listFiles();
        java.util.Arrays.sort(list, new Comparator<File>() {

            @Override
            public int compare(File o1, File o2) {
                return (int) (o1.length() - o2.length());
            }
        });
        for (int i = 0; i < list.length; i++) {
            if (list[i].getPath().endsWith(".shp")) {
                List files = new ArrayList();
                for (int j = i + 1; j < list.length; j++) {
                    if (list[j].getPath().endsWith(".shp")) {
                       /* File out = new File(outputDir + "/intersection_" + list[i].getName() + "_" + list[j].getName() + ".txt");
                        File outZip = new File(outputDir + "/intersection_" + list[i].getName() + "_" + list[j].getName() + ".zip");
                        File out2 = new File(outputDir + "/intersection_" + list[j].getName() + "_" + list[i].getName() + ".txt");
                        File outZip2 = new File(outputDir + "/intersection_" + list[j].getName() + "_" + list[i].getName() + ".zip");
                        if (!out.exists() && !outZip.exists() &&
                                !out2.exists() && !outZip2.exists()) {
                            files.add(list[j].getPath());
                        }*/
                    }
                }
                //intersectShapefiles(list[i].getPath(), files);
            }
        }
    }

    /**
     * intersects filename1 with all files in List filenames2 to produce an intersection_filename1_* file for
     * each file in filename2.
     */
    static void intersectShapefilesInDir(String shapefileDir, List intersectionFileNames, String outputDir) {
        File f = new File("/data/ala/data/layers/ready/shape/");
        File[] list = f.listFiles();
        java.util.Arrays.sort(list, new Comparator<File>() {

            @Override
            public int compare(File o1, File o2) {
                return (int) (o1.length() - o2.length());
            }
        });
        for (int i = 0; i < list.length; i++) {
            if (list[i].getPath().endsWith(".shp")) {
                List files = new ArrayList();
                for (int j = i + 1; j < list.length; j++) {
                    if (list[j].getPath().endsWith(".shp")) {
                        File out = new File(outputDir + "/intersection_" + list[i].getName() + "_" + list[j].getName() + ".txt");
                        File outZip = new File(outputDir + "/intersection_" + list[i].getName() + "_" + list[j].getName() + ".zip");
                        File out2 = new File(outputDir + "/intersection_" + list[j].getName() + "_" + list[i].getName() + ".txt");
                        File outZip2 = new File(outputDir + "/intersection_" + list[j].getName() + "_" + list[i].getName() + ".zip");
                        if (!out.exists() && !outZip.exists() &&
                                !out2.exists() && !outZip2.exists()) {
                            files.add(list[j].getPath());
                        }
                    }
                }
                intersectShapefiles(list[i].getPath(), files, outputDir);
            }
        }
    }

    /**
     * intersects filename1 with all files in List filenames2 to produce an intersection_filename1_* file for
     * each file in filename2.
     *
     * @param filename1
     * @param filenames2
     */
    static void intersectShapefiles(String filename1, List filenames2, String outputDir) {
        try {

            final LinkedBlockingQueue lbq = new LinkedBlockingQueue();
            final AtomicInteger ai = new AtomicInteger(0);

            FeatureSource source1;
            FeatureSource source2;

            Map map1 = new HashMap();
            map1.put("url", new File(filename1).toURI().toURL());
            DataStore dataStore1 = DataStoreFinder.getDataStore(map1);
            String typeName1 = dataStore1.getTypeNames()[0];
            source1 = dataStore1.getFeatureSource(typeName1);

            FeatureIterator iterator1 = source1.getFeatures().features();

            System.out.println(filename1 + ": shape count=" + source1.getCount(Query.ALL));

            int count1 = 0;

            List geoms1 = new ArrayList();
            List pgeoms1 = new ArrayList();
            List ids1 = new ArrayList();
            while (iterator1.hasNext()) {
                count1++;

                SimpleFeature feature1 = (SimpleFeature) iterator1.next();
                Geometry geom1 = (Geometry) feature1.getDefaultGeometry();
                BoundingBox bb = feature1.getBounds();

                //if geom1 is large, split it and iterate
                int len = geom1.toText().length();

                Geometry[] glist = {geom1};
                if (len > 1000000) {
                    glist = split(geom1, 4, 4);
                }
                for (int n = 0; n < glist.length; n++) {
                    try {
                        if (glist[n].getArea() > 0) {
                            geoms1.add(glist[n]);
                            pgeoms1.add(PreparedGeometryFactory.prepare(glist[n]));
                            ids1.add(feature1.getID());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            for (int h = 0; h < filenames2.size(); h++) {
                long startTime = System.currentTimeMillis();

                File outf = new File(outputDir + "/intersection_" + new File(filename1).getName() + "_" + new File((String) filenames2.get(h)).getName() + ".txt");
                File outZip = new File(outputDir + "/intersection_" + new File(filename1).getName() + "_" + new File((String) filenames2.get(h)).getName() + ".zip");

                BufferedWriter out = new BufferedWriter(new FileWriter(outf));
                Map map2 = new HashMap();
                map2.put("url", new File((String) filenames2.get(h)).toURI().toURL());
                DataStore dataStore2 = DataStoreFinder.getDataStore(map2);
                String typeName2 = dataStore2.getTypeNames()[0];
                source2 = dataStore2.getFeatureSource(typeName2);
                System.out.println((String) filenames2.get(h) + ": shape count=" + source2.getCount(Query.ALL));

                //fetch all feature 2 bounding boxes
                FeatureIterator iterator2 = source2.getFeatures().features();

                double[] bb2 = new double[4 * source2.getCount(Query.ALL)];
                String[] id2 = new String[source2.getCount(Query.ALL)];
                Geometry[] g2 = new Geometry[source2.getCount(Query.ALL)];
                PreparedGeometry[] pg2 = new PreparedGeometry[source2.getCount(Query.ALL)];
                int i = 0;
                while (iterator2.hasNext()) {
                    SimpleFeature feature2 = (SimpleFeature) iterator2.next();

                    BoundingBox b = feature2.getBounds();
                    bb2[i] = b.getMinX();
                    bb2[i + 1] = b.getMinY();
                    bb2[i + 2] = b.getMaxX();
                    bb2[i + 3] = b.getMaxY();

                    id2[i / 4] = feature2.getID();
                    g2[i / 4] = (Geometry) feature2.getDefaultGeometry();
                    pg2[i / 4] = PreparedGeometryFactory.prepare(g2[i / 4]);

                    i += 4;
                }

                //compare to all in (2)
                for (int n = 0; n < geoms1.size(); n++) {
                    Geometry geom1 = (Geometry) geoms1.get(n);
                    //PreparedGeometry pg1 = PreparedGeometryFactory.prepare(geom1);
                    Envelope e = geom1.getEnvelopeInternal();

                    for (int j = 0; j < g2.length; j++) {
                        int jj = j * 4;
                        if (bb2[jj] <= e.getMaxX() && bb2[jj + 2] >= e.getMinX() &&
                                bb2[jj + 1] <= e.getMaxY() && bb2[jj + 3] >= e.getMinY()) {

                            Object[] newtask = new Object[7];
                            newtask[0] = geom1;
                            newtask[1] = g2[j];
                            newtask[2] = out;
                            newtask[3] = ids1.get(n);
                            newtask[4] = id2[j];
                            newtask[5] = pgeoms1.get(n);
                            newtask[6] = pg2[j];

                            lbq.put(newtask);
                        }
                    }
                }

                System.out.println("comparisons required: " + lbq.size());

                //wait until finished
                final CountDownLatch cdl = new CountDownLatch(lbq.size());
                final Thread[] threads = new Thread[4];
                for (int j = 0; j < threads.length; j++) {
                    threads[j] = new Thread() {
                        @Override
                        public void run() {
                            try {
                                long timePos = System.currentTimeMillis();

                                while (true) {
                                    Object[] os = (Object[]) lbq.take();
                                    if (lbq.size() % 5000 == 0) {
                                        System.out.print("(" + lbq.size() + ")");
                                    }

                                    Geometry geom1 = (Geometry) os[0];
                                    Geometry g2 = (Geometry) os[1];
                                    Writer out = (Writer) os[2];
                                    String id1 = (String) os[3];
                                    String id2 = (String) os[4];
                                    PreparedGeometry pg1 = (PreparedGeometry) os[5];
                                    PreparedGeometry pg2 = (PreparedGeometry) os[6];

                                    try {
                                        if (pg1.within(g2)) {
                                            writeIntersection(out, id1, id2, geom1);
                                            ai.incrementAndGet();
                                        } else if (pg2.within(geom1)) {
                                            writeIntersection(out, id1, id2, g2);
                                            //bb2[jj] = 1000; //avoids future comparisons
                                            ai.incrementAndGet();
                                        } else if (pg1.intersects(g2)) {
                                            Geometry intersection = geom1.intersection(g2);
                                            if (intersection.getArea() > 0) {
                                                writeIntersection(out, id1, id2, intersection);
                                                ai.incrementAndGet();
                                            }
                                        }
                                    } catch (Exception e) {
                                        writeIntersection(out, id1, id2 + ", error: " + e.getMessage(), null);
                                    }
                                    cdl.countDown();
                                }
                            } catch (InterruptedException e) {

                            } catch (Exception e) {
                                e.printStackTrace();
                                cdl.countDown();
                            }
                        }
                    };

                    threads[j].start();
                }

                cdl.await();

                for (int j = 0; j < threads.length; j++) {
                    threads[j].interrupt();
                }
                iterator2.close();
                dataStore2.dispose();

                out.flush();
                out.close();

                System.out.println("total time: " + outf.getName() + " = " + (System.currentTimeMillis() - startTime) + "ms");
                //zip
                try {
                    ZipUtil.zip(outZip.getPath(), new String[]{outf.getPath()});
                    FileUtils.deleteQuietly(outf);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            iterator1.close();
            dataStore1.dispose();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static synchronized void writeIntersection(Writer out, String id1, String id2, Geometry g) throws Exception {
        out.write(id1);
        out.write("\n");
        out.write(id2);
        out.write("\n");
        if (g != null) {
            out.write(g.toText());
        }
        out.write("\n");
    }

    static Geometry[] split(Geometry geom, int xsize, int ysize) {
        Envelope bb = geom.getEnvelopeInternal();

        //split into pieces
        double[] xs = new double[xsize + 1];
        xs[0] = bb.getMinX();
        for (int i = 1; i < xsize; i++) {
            xs[i] = bb.getMinX() + (bb.getMaxX() - bb.getMinX()) * i / (double) xsize;
        }
        xs[xsize] = bb.getMaxX();

        double[] ys = new double[ysize + 1];
        ys[0] = bb.getMinY();
        for (int i = 1; i < ysize; i++) {
            ys[i] = bb.getMinY() + (bb.getMaxY() - bb.getMinY()) * i / (double) ysize;
        }
        ys[ysize] = bb.getMaxY();

        Geometry[] glist = new Geometry[xsize * ysize];
        int gi = 0;
        for (int x = 0; x < xsize; x++) {
            for (int y = 0; y < ysize; y++) {
                GeometryBuilder gb = new GeometryBuilder();
                Geometry g = gb.box(xs[x], xs[x + 1], ys[y], ys[y + 1]);

                glist[gi] = g.intersection(geom);
                gi++;
            }
        }

        return glist;
    }
}
