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
package au.org.ala.layers.intersect;

import au.org.ala.layers.dao.IntersectCallback;
import au.org.ala.layers.dto.GridClass;
import au.org.ala.layers.dto.IntersectionFile;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Adam
 */
public class SamplingThread extends Thread {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(SamplingThread.class);
    LinkedBlockingQueue<Integer> lbq;
    CountDownLatch cdl;
    double[][] points;
    IntersectionFile[] intersectionFiles;
    ArrayList<String> output;
    int threadCount;
    SimpleShapeFileCache simpleShapeFileCache;
    int gridBufferSize;
    IntersectCallback callback;

    public SamplingThread(LinkedBlockingQueue<Integer> lbq, CountDownLatch cdl, IntersectionFile[] intersectionFiles,
                          double[][] points, ArrayList<String> output, int threadCount,
                          SimpleShapeFileCache simpleShapeFileCache, int gridBufferSize, IntersectCallback callback) {
        this.lbq = lbq;
        this.cdl = cdl;
        this.points = points;
        this.intersectionFiles = intersectionFiles;
        this.output = output;
        this.threadCount = threadCount;
        this.simpleShapeFileCache = simpleShapeFileCache;
        this.gridBufferSize = gridBufferSize;
        this.callback = callback;
        setPriority(MIN_PRIORITY);
    }

    public void run() {
        try {
            while (true) {
                int pos = lbq.take();

                this.callback.setCurrentLayerIdx(pos);

                try {
                    StringBuilder sb = new StringBuilder();
                    sample(points, intersectionFiles[pos], sb);
                    output.set(pos, sb.toString());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }

                this.callback.setCurrentLayer(intersectionFiles[pos]);
                cdl.countDown();
            }
        } catch (Exception e) {
            logger.trace(e.getMessage(), e);
        }
    }

    public void sample(double[][] points, IntersectionFile intersectionFile, StringBuilder sb) {
        if (intersectionFile == null) {
            return;
        }
        HashMap<Integer, GridClass> classes = intersectionFile.getClasses();
        String shapeFieldName = intersectionFile.getShapeFields();
        String fileName = intersectionFile.getFilePath();
        String name = intersectionFile.getFieldId();
        long start = System.currentTimeMillis();
        logger.info("Starting sampling " + points.length + " points in " + name + ":"
                + fileName + (shapeFieldName == null ? "" : " field: " + shapeFieldName));
        callback.progressMessage("Started sampling layer:" + intersectionFile.getLayerName());
        if (StringUtils.isNotEmpty(shapeFieldName)) {
            intersectShape(fileName, shapeFieldName, points, sb);
        } else if (classes != null) {
            intersectGridAsContextual(fileName, classes, points, sb);
        } else {
            intersectGrid(fileName, points, sb);
        }

        logger.info("Finished sampling " + points.length + " points in " + name + ":"
                + fileName + " in " + (System.currentTimeMillis() - start) + "ms");

        callback.progressMessage("Finished sampling layer: " + intersectionFile.getLayerName() + ". Points processed: " + points.length / 2);
    }

    public void intersectGrid(String filename, double[][] points, StringBuilder sb) {
        try {
            Grid grid = new Grid(filename, true);
            float[] values = grid.getValues3(points, gridBufferSize);

            if (values != null) {
                for (int i = 0; i < points.length; i++) {
                    if (i > 0) {
                        sb.append("\n");
                    }
                    if (!Float.isNaN(values[i])) {
                        sb.append(values[i]);
                    } else {
                        sb.append("");
                    }
                }
            } else {
                for (int i = 1; i < points.length; i++) {
                    sb.append("\n");
                }
            }
        } catch (Exception e) {
            logger.error("Error with grid: " + filename, e);
        }
    }

    public void intersectGridAsContextual(String filename, HashMap<Integer, GridClass> classes, double[][] points, StringBuilder sb) {
        try {
            Grid grid = new Grid(filename);
            GridClass gc;
            float[] values = grid.getValues3(points, gridBufferSize);

            if (values != null) {
                for (int i = 0; i < points.length; i++) {
                    if (i > 0) {
                        sb.append("\n");
                    }
                    gc = classes.get((int) values[i]);
                    if (gc != null) {
                        sb.append(gc.getName());
                    } else {
                        sb.append("");
                    }
                }
            } else {
                for (int i = 1; i < points.length; i++) {
                    sb.append("\n");
                }
            }
        } catch (Exception e) {
            logger.error("Error with grid: " + filename, e);
        }
    }

    void intersectShape(String filename, String fieldName, double[][] points, StringBuilder sb) {
        try {
            SimpleShapeFile ssf = null;

            if (simpleShapeFileCache != null) {
                ssf = simpleShapeFileCache.get(filename);
            }

            if (ssf == null) {
                logger.debug("shape file not in cache: " + filename);
                ssf = new SimpleShapeFile(filename, fieldName);
            }

            int column_idx = ssf.getColumnIdx(fieldName);
            String[] categories = ssf.getColumnLookup(column_idx);

            int[] values = ssf.intersect(points, categories, column_idx, threadCount);

            if (values != null) {
                for (int i = 0; i < values.length; i++) {
                    if (i > 0) {
                        sb.append("\n");
                    }
                    if (values[i] >= 0) {
                        sb.append(categories[values[i]]);
                    } else {
                        sb.append("");
                    }
                }
            } else {
                for (int i = 1; i < points.length; i++) {
                    sb.append("\n");
                }
            }
        } catch (Exception e) {
            logger.error("Error with shapefile: " + filename, e);
        }
    }
}
