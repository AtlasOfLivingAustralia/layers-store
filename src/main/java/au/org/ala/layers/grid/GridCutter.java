/**
 * ************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 * the specific language governing rights and limitations under the License.
 * *************************************************************************
 */
package au.org.ala.layers.grid;

import au.org.ala.layers.client.Client;
import au.org.ala.layers.intersect.Grid;
import au.org.ala.layers.intersect.IntersectConfig;
import au.org.ala.layers.intersect.SimpleRegion;
import au.org.ala.layers.intersect.SimpleShapeFile;
import au.org.ala.layers.util.LayerFilter;
import au.org.ala.layers.util.SpatialUtil;

import java.io.File;
import java.io.FileWriter;
import java.util.TreeMap;

/**
 * Class for region cutting test data grids
 *
 * @author adam
 */
public class GridCutter {

    /**
     * exports a list of layers cut against a region
     * <p/>
     * Cut layer files generated are input layers with grid cells outside of
     * region set as missing.
     *
     * @param layers          list of layer fieldIds to be cut as String[].
     * @param resolution      target resolution as String
     * @param region          null or region to cut against as SimpleRegion. Cannot be
     *                        used with envelopes.
     * @param envelopes       nul or region to cut against as LayerFilter[]. Cannot be
     *                        used with region.
     * @param extentsFilename output filename and path for writing output
     *                        extents.
     * @return directory containing the cut grid files.
     */
    public static String cut2(String[] layers, String resolution, SimpleRegion region, LayerFilter[] envelopes, String extentsFilename) {
        //check if resolution needs changing
        resolution = confirmResolution(layers, resolution);

        //get extents for all layers
        double[][] extents = getLayerExtents(resolution, layers[0]);
        for (int i = 1; i < layers.length; i++) {
            extents = internalExtents(extents, getLayerExtents(resolution, layers[i]));
            if (!isValidExtents(extents)) {
                return null;
            }
        }
        //do extents check for contextual envelopes as well
        if (envelopes != null) {
            extents = internalExtents(extents, getLayerFilterExtents(envelopes));
            if (!isValidExtents(extents)) {
                return null;
            }
        }

        //get mask and adjust extents for filter
        byte[][] mask;
        int w = 0, h = 0;
        double res = Double.parseDouble(resolution);
        if (region != null) {
            extents = internalExtents(extents, region.getBoundingBox());

            if (!isValidExtents(extents)) {
                return null;
            }

            h = (int) Math.ceil((extents[1][1] - extents[0][1]) / res);
            w = (int) Math.ceil((extents[1][0] - extents[0][0]) / res);
            mask = getRegionMask(res, extents, w, h, region);
        } else if (envelopes != null) {
            h = (int) Math.ceil((extents[1][1] - extents[0][1]) / res);
            w = (int) Math.ceil((extents[1][0] - extents[0][0]) / res);
            mask = getEnvelopeMaskAndUpdateExtents(resolution, res, extents, h, w, envelopes);
            h = (int) Math.ceil((extents[1][1] - extents[0][1]) / res);
            w = (int) Math.ceil((extents[1][0] - extents[0][0]) / res);
        } else {
            h = (int) Math.ceil((extents[1][1] - extents[0][1]) / res);
            w = (int) Math.ceil((extents[1][0] - extents[0][0]) / res);
            mask = getMask(res, extents, w, h);
        }

        //mkdir in index location
        String newPath = null;
        try {
            newPath = IntersectConfig.getAnalysisTmpLayerFilesPath() + System.currentTimeMillis() + File.separator;
            File directory = new File(newPath);
            directory.mkdir();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //apply mask
        for (int i = 0; i < layers.length; i++) {
            applyMask(newPath, resolution, extents, w, h, mask, layers[i]);
        }

        //write extents file
        writeExtents(extentsFilename, extents, w, h);

        return newPath;
    }

    static double[][] internalExtents(double[][] e1, double[][] e2) {
        double[][] internalExtents = new double[2][2];

        internalExtents[0][0] = Math.max(e1[0][0], e2[0][0]);
        internalExtents[0][1] = Math.max(e1[0][1], e2[0][1]);
        internalExtents[1][0] = Math.min(e1[1][0], e2[1][0]);
        internalExtents[1][1] = Math.min(e1[1][1], e2[1][1]);

        return internalExtents;
    }

    static boolean isValidExtents(double[][] e) {
        return e[0][0] < e[1][0] && e[0][1] < e[1][1];
    }

    static double[][] getLayerExtents(String resolution, String layer) {
        double[][] extents = new double[2][2];

        if (getLayerPath(resolution, layer) == null
                && "c".equalsIgnoreCase(Client.getFieldDao().getFieldById(layer).getType())) {
            //use world extents here, remember to do object extents later.
            extents[0][0] = -180;
            extents[0][1] = -90;
            extents[1][0] = 180;
            extents[1][1] = 90;

        } else {
            Grid g = Grid.getGrid(getLayerPath(resolution, layer));

            extents[0][0] = g.xmin;
            extents[0][1] = g.ymin;
            extents[1][0] = g.xmax;
            extents[1][1] = g.ymax;
        }

        return extents;
    }

    public static String getLayerPath(String resolution, String layer) {
        String field = Client.getLayerIntersectDao().getConfig().getIntersectionFile(layer).getFieldId();

        File file = new File(IntersectConfig.getAnalysisLayerFilesPath() + File.separator + resolution + File.separator + field + ".grd");

        //move up a resolution when the file does not exist at the target resolution
        try {
            while (!file.exists()) {
                TreeMap<Double, String> resolutionDirs = new TreeMap<Double, String>();
                for (File dir : new File(IntersectConfig.getAnalysisLayerFilesPath()).listFiles()) {
                    if (dir.isDirectory()) {
                        try {
                            resolutionDirs.put(Double.parseDouble(dir.getName()), dir.getName());
                        } catch (Exception e) {
                        }
                    }
                }

                String newResolution = resolutionDirs.higherEntry(Double.parseDouble(resolution)).getValue();

                if (newResolution.equals(resolution)) {
                    break;
                } else {
                    resolution = newResolution;
                    file = new File(IntersectConfig.getAnalysisLayerFilesPath() + File.separator + resolution + File.separator + field + ".grd");
                }
            }
        } catch (Exception e) {
        }

        String layerPath = IntersectConfig.getAnalysisLayerFilesPath() + File.separator + resolution + File.separator + field;

        if (new File(layerPath + ".grd").exists()) {
            return layerPath;
        } else {
            //look for an analysis layer
            String[] info = Client.getLayerIntersectDao().getConfig().getAnalysisLayerInfo(layer);
            if (info != null) {
                return info[1];
            } else {
                System.out.println("getLayerPath, cannot find for: " + layer + ", " + resolution);
                return null;
            }
        }
    }


    public static boolean existsLayerPath(String resolution, String layer, boolean do_not_lower_resolution) {
        String field = Client.getLayerIntersectDao().getConfig().getIntersectionFile(layer).getFieldId();

        File file = new File(IntersectConfig.getAnalysisLayerFilesPath() + File.separator + resolution + File.separator + field + ".grd");

        //move up a resolution when the file does not exist at the target resolution
        try {
            while (!file.exists() && !do_not_lower_resolution) {
                TreeMap<Double, String> resolutionDirs = new TreeMap<Double, String>();
                for (File dir : new File(IntersectConfig.getAnalysisLayerFilesPath()).listFiles()) {
                    if (dir.isDirectory()) {
                        try {
                            resolutionDirs.put(Double.parseDouble(dir.getName()), dir.getName());
                        } catch (Exception e) {
                        }
                    }
                }

                String newResolution = resolutionDirs.higherEntry(Double.parseDouble(resolution)).getValue();

                if (newResolution.equals(resolution)) {
                    break;
                } else {
                    resolution = newResolution;
                    file = new File(IntersectConfig.getAnalysisLayerFilesPath() + File.separator + resolution + File.separator + field + ".grd");
                }
            }
        } catch (Exception e) {
        }

        String layerPath = IntersectConfig.getAnalysisLayerFilesPath() + File.separator + resolution + File.separator + field;

        if (new File(layerPath + ".grd").exists()) {
            return true;
        } else {
            //look for an analysis layer
            String[] info = Client.getLayerIntersectDao().getConfig().getAnalysisLayerInfo(layer);
            if (info != null) {
                return true;
            } else {
                return false;
            }
        }
    }

    static void applyMask(String dir, String resolution, double[][] extents, int w, int h, byte[][] mask, String layer) {
        //layer output container
        double[] dfiltered = new double[w * h];

        //open grid and get all data
        Grid grid = Grid.getGrid(getLayerPath(resolution, layer));
        float[] d = grid.getGrid(); //get whole layer

        //set all as missing values
        for (int i = 0; i < dfiltered.length; i++) {
            dfiltered[i] = Double.NaN;
        }

        double res = Double.parseDouble(resolution);

        for (int i = 0; i < mask.length; i++) {
            for (int j = 0; j < mask[0].length; j++) {
                if (mask[i][j] > 0) {
                    dfiltered[j + (h - i - 1) * w] = grid.getValues3(new double[][]{{j * res + extents[0][0], i * res + extents[0][1]}}, 40960)[0];
                }
            }
        }

        grid.writeGrid(dir + layer, dfiltered,
                extents[0][0],
                extents[0][1],
                extents[1][0],
                extents[1][1],
                res, res, h, w);
    }

    static void writeExtents(String filename, double[][] extents, int w, int h) {
        if (filename != null) {
            try {
                FileWriter fw = new FileWriter(filename);
                fw.append(String.valueOf(w)).append("\n");
                fw.append(String.valueOf(h)).append("\n");
                fw.append(String.valueOf(extents[0][0])).append("\n");
                fw.append(String.valueOf(extents[0][1])).append("\n");
                fw.append(String.valueOf(extents[1][0])).append("\n");
                fw.append(String.valueOf(extents[1][1]));
                fw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Get a region mask.
     * <p/>
     * Note: using decimal degree grid, probably should be EPSG900913 grid.
     *
     * @param res     resolution as double
     * @param extents extents as double[][] with [0][0]=xmin, [0][1]=ymin,
     *                [1][0]=xmax, [1][1]=ymax.
     * @param h       height as int.
     * @param w       width as int.
     * @param region  area for the mask as SimpleRegion.
     * @return
     */
    private static byte[][] getRegionMask(double res, double[][] extents, int w, int h, SimpleRegion region) {
        byte[][] mask = new byte[h][w];

        //can also use region.getOverlapGridCells_EPSG900913
        region.getOverlapGridCells(extents[0][0], extents[0][1], extents[1][0], extents[1][1], w, h, mask);
        for (int i = 0; i < h; i++) {
            for (int j = 0; j < w; j++) {
                //double tx = (j + 0.5) * res + extents[0][0];
                //double ty = (i + 0.5) * res + extents[0][1];
                //if (region.isWithin_EPSG900913(tx, ty)) {
                //    mask[i][j] = 1;
                //}
                if (mask[i][j] > 0) {
                    mask[i][j] = 1;
                }
            }
        }
        return mask;
    }

    private static byte[][] getMask(double res, double[][] extents, int w, int h) {
        byte[][] mask = new byte[h][w];
        for (int i = 0; i < h; i++) {
            for (int j = 0; j < w; j++) {
                mask[i][j] = 1;
            }
        }
        return mask;
    }

    /**
     * Get a mask, 0=absence, 1=presence, for a given envelope and extents.
     *
     * @param resolution resolution as String.
     * @param res        resultions as double.
     * @param extents    extents as double[][] with [0][0]=xmin, [0][1]=ymin,
     *                   [1][0]=xmax, [1][1]=ymax.
     * @param h          height as int.
     * @param w          width as int.
     * @param envelopes
     * @return mask as byte[][]
     */
    private static byte[][] getEnvelopeMaskAndUpdateExtents(String resolution, double res, double[][] extents, int h, int w, LayerFilter[] envelopes) {
        byte[][] mask = new byte[h][w];

        double[][] points = new double[h * w][2];
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                points[i + j * w][0] = (double) (extents[0][0] + (i + 0.5) * res);
                points[i + j * w][1] = (double) (extents[0][1] + (j + 0.5) * res);
                //mask[j][i] = 0;
            }
        }

        for (int k = 0; k < envelopes.length; k++) {
            LayerFilter lf = envelopes[k];

            // if it is contextual and a grid file does not exist at the requested resolution
            // and it is not a grid processed as a shape file,
            // then get the shape file to do the intersection
            if (existsLayerPath(resolution, lf.getLayername(), true) && lf.isContextual()
                    && "c".equalsIgnoreCase(Client.getFieldDao().getFieldById(lf.getLayername()).getType())) {

                String[] ids = lf.getIds();
                SimpleRegion[] srs = new SimpleRegion[ids.length];

                for (int i = 0; i < ids.length; i++) {
                    srs[i] = SimpleShapeFile.parseWKT(Client.getObjectDao().getObjectsGeometryById(ids[i], "WKT"));

                }
                for (int i = 0; i < points.length; i++) {
                    for (int j = 0; j < srs.length; j++) {
                        if (srs[j].isWithin(points[i][0], points[i][1])) {
                            mask[i / w][i % w]++;
                            break;
                        }
                    }
                }
            } else {
                Grid grid = Grid.getGrid(getLayerPath(resolution, lf.getLayername()));

                float[] d = grid.getValues3(points, 40960);

                for (int i = 0; i < d.length; i++) {
                    if (lf.isValid(d[i])) {
                        mask[i / w][i % w]++;
                    }
                }
            }
        }

        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                if (mask[j][i] == envelopes.length) {
                    mask[j][i] = 1;
                } else {
                    mask[j][i] = 0;
                }
            }
        }

        //find internal extents
        int minx = w;
        int maxx = -1;
        int miny = h;
        int maxy = -1;
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                if (mask[j][i] > 0) {
                    if (minx > i) {
                        minx = i;
                    }
                    if (maxx < i) {
                        maxx = i;
                    }
                    if (miny > j) {
                        miny = j;
                    }
                    if (maxy < j) {
                        maxy = j;
                    }
                }
            }
        }

        //reduce the size of the mask
        int nw = maxx - minx + 1;
        int nh = maxy - miny + 1;
        byte[][] smallerMask = new byte[nh][nw];
        for (int i = minx; i < maxx; i++) {
            for (int j = miny; j < maxy; j++) {
                smallerMask[j - miny][i - minx] = mask[j][i];
            }
        }


        //update extents, must never be larger than the original extents (res is not negative, minx maxx miny mazy are not negative and < w & h respectively
        extents[0][0] = Math.max(extents[0][0] + minx * res, extents[0][0]); //min x value
        extents[1][0] = Math.min(extents[1][0] - (w - maxx - 1) * res, extents[1][0]); //max x value
        extents[0][1] = Math.max(extents[0][1] + miny * res, extents[0][1]); //min y value
        extents[1][1] = Math.min(extents[1][1] - (h - maxy - 1) * res, extents[1][1]); //max y value

        return smallerMask;
    }

    /**
     * Write a diva grid to disk for the envelope, 0 = absence, 1 = presence.
     *
     * @param filename   output filename for the grid as String.
     * @param resolution target resolution in decimal degrees as String.
     * @param envelopes  envelope specification as LayerFilter[].
     * @return area in sq km as double.
     */
    public static double makeEnvelope(String filename, String resolution, LayerFilter[] envelopes, long maxGridCount) {

        //get extents for all layers
        double[][] extents = getLayerExtents(resolution, envelopes[0].getLayername());
        for (int i = 1; i < envelopes.length; i++) {
            extents = internalExtents(extents, getLayerExtents(resolution, envelopes[i].getLayername()));
            if (!isValidExtents(extents)) {
                return -1;
            }
        }
        //do extents check for contextual envelopes as well
        extents = internalExtents(extents, getLayerFilterExtents(envelopes));
        if (!isValidExtents(extents)) {
            return -1;
        }

        double res = Double.parseDouble(resolution);

        //limit the size of the grid files that can be generated
        while ((Math.abs(extents[0][1] - extents[1][0]) / res) * (Math.abs(extents[0][0] - extents[1][0]) / res) > maxGridCount * 2.0) {
            res = res * 2;
        }
        if (res != Double.parseDouble(resolution)) {
            resolution = String.format("%f", res);
        }

        //get mask and adjust extents for filter
        byte[][] mask;
        int w, h;
        h = (int) Math.ceil((extents[1][1] - extents[0][1]) / res);
        w = (int) Math.ceil((extents[1][0] - extents[0][0]) / res);
        mask = getEnvelopeMaskAndUpdateExtents(resolution, res, extents, h, w, envelopes);
        h = (int) Math.ceil((extents[1][1] - extents[0][1]) / res);
        if (((int) Math.ceil((extents[1][1] + res - extents[0][1]) / res)) == h) {
            extents[1][1] += res;
        }
        w = (int) Math.ceil((extents[1][0] - extents[0][0]) / res);
        if (((int) Math.ceil((extents[1][0] + res - extents[0][0]) / res)) == w) {
            extents[1][0] += res;
        }

        float[] values = new float[w * h];
        int pos = 0;
        double areaSqKm = 0;
        for (int i = h - 1; i >= 0; i--) {
            for (int j = 0; j < w; j++) {
                if (i < mask.length && j < mask[i].length) {
                    values[pos] = mask[i][j];

                    if (mask[i][j] > 0) {
                        areaSqKm += SpatialUtil.cellArea(res, extents[0][1] + res * i);
                    }
                } else {
                    values[pos] = 0;
                }
                pos++;
            }
        }

        Grid grid = new Grid(getLayerPath(resolution, envelopes[0].getLayername()));

        grid.writeGrid(filename, values,
                extents[0][0],
                extents[0][1],
                extents[1][0],
                extents[1][1],
                res, res, h, w);

        return areaSqKm;
    }

    private static double[][] getLayerFilterExtents(LayerFilter[] envelopes) {

        double[][] extents = new double[][]{{-180, -90}, {180, 90}};
        for (int i = 0; i < envelopes.length; i++) {
            if ("c".equalsIgnoreCase(Client.getFieldDao().getFieldById(envelopes[i].getLayername()).getType())) {
                String[] ids = envelopes[i].getIds();
                for (int j = 0; j < ids.length; j++) {
                    try {
                        double[][] bbox = SimpleShapeFile.parseWKT(Client.getObjectDao().getObjectByPid(ids[j]).getBbox()).getBoundingBox();
                        extents = internalExtents(extents, bbox);
                    } catch (Exception e) {
                        //Expecting this to fail often!
                        e.printStackTrace();
                    }

                }
            }
        }
        return extents;
    }


    /**
     * Test if the layer filter is valid.
     * <p/>
     * The common problem is that a filter may refer to a layer that is not
     * available.
     *
     * @param resolution target resolution as String.
     * @param filter     layer filter as LayerFilter[].
     * @return true iff valid filter.
     */
    public static boolean isValidLayerFilter(String resolution, LayerFilter[] filter) {
        for (LayerFilter lf : filter) {
            //it is not valid if the layer itself does not exist.
            // so if there is not grid file available to GridCutter
            // and the layer is not a contextual layer of type 'c' (ie, not a grid file based contextual layer)
            // it is not valid
            if (GridCutter.getLayerPath(resolution, lf.getLayername()) == null
                    && !(lf.isContextual() && Client.getFieldDao().getFieldById(lf.getLayername()).getType().equalsIgnoreCase("c"))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determine the grid resolution that will be in use.
     *
     * @param layers     list of layers to be used as String []
     * @param resolution target resolution as String
     * @return resolution that will be used
     */
    private static String confirmResolution(String[] layers, String resolution) {
        try {
            TreeMap<Double, String> resolutions = new TreeMap<Double, String>();
            for (String layer : layers) {
                String path = GridCutter.getLayerPath(resolution, layer);
                int end, start;
                if (path != null
                        && ((end = path.lastIndexOf(File.separator)) > 0)
                        && ((start = path.lastIndexOf(File.separator, end - 1)) > 0)) {
                    String res = path.substring(start + 1, end);
                    Double d = Double.parseDouble(res);
                    if (d < 1) {
                        resolutions.put(d, res);
                    }
                }
            }
            if (resolutions.size() > 0) {
                resolution = resolutions.firstEntry().getValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resolution;
    }
}
