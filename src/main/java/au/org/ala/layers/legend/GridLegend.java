package au.org.ala.layers.legend;


import au.org.ala.layers.intersect.Grid;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;

public class GridLegend {

    private static final Logger logger = Logger.getLogger(GridLegend.class);

    /**
     * @param filename    grid file name.  must reside in
     *                    tabulation settings <environmental_data_path> as String
     * @param output_name Base output file path and name as String
     */
    public GridLegend(String filename, String output_name, boolean useAreaEvaluation, String[] legendNames, FileWriter cutpointFile, int scaleDown, boolean minAsTransparent) {
        Grid g = new Grid(filename);

        if (legendNames != null) {
            java.util.Arrays.sort(legendNames);
        }

        //don't bother reading the whole file
        int sampleInterval = (g.ncols / 1000.0) * (g.nrows / 1000.0) < 0.128 ? 1 : (int) ((g.ncols / 1000.0) * (g.nrows / 1000.0) / 0.128);
        float[] d = g.getGrid(sampleInterval);

        if (legendNames != null) {
            java.util.Arrays.sort(legendNames);
        }

        java.util.Arrays.sort(d);

        //drop NaNs
        int firstNaN = d.length;
        for (int i = d.length - 1; i >= 0; i--) {
            if (Float.isNaN(d[i])) {
                firstNaN = i;
            } else {
                break;
            }
        }
        if (firstNaN < d.length) {
            float[] copy = new float[firstNaN];
            System.arraycopy(d, 0, copy, 0, firstNaN);
            d = copy;
        }

        //min/max correction
        d[0] = (float) g.minval;
        d[d.length - 1] = (float) g.maxval;


        Legend[] legends = new Legend[1];
        legends[0] = new LegendEqualArea();
//        legends[3] = new LegendEqualSize();
//        legends[1] = new LegendEvenInterval();
//        legends[2] = new LegendEvenIntervalLog();
//        legends[4] = new LegendEvenIntervalLog10();

        int minI = 0;
        double minE = 0;
        boolean firstTime = true;
        for (int i = 0; i < legends.length; i++) {
            if (legendNames == null || java.util.Arrays.binarySearch(legendNames, legends[i].getTypeName()) < 0) {
                continue;
            }
            legends[i].generate(d);
            legends[i].determineGroupSizes(d);
            double e2 = 0;
            if (useAreaEvaluation) {
                e2 = legends[i].evaluateStdDevArea(d);
            } else {
                e2 = legends[i].evaluateStdDev(d);
            }
            try {
                (new File(output_name + ".png")).delete();
            } catch (Exception e) {
            }

            //must 'unsort' d
            d = null;
            g = null;
            System.gc();
            g = new Grid(filename);
            d = g.getGrid(sampleInterval);
            if (sampleInterval > 1) {
                logger.info("test output image is messed up because of >1 sample interval (large file)");
            }
            if (g.ncols > 0) {
                legends[i].exportImage(d, g.ncols, output_name + ".png", Math.max(scaleDown, g.ncols / 50), minAsTransparent);
            } else {
                legends[i].exportImage(d, 500, output_name + ".png", Math.max(scaleDown, 500 / 50), minAsTransparent);
            }
            legends[i].exportLegend(output_name + "_legend.txt");

            legends[i].exportSLD(g, output_name + ".sld", g.units, true, minAsTransparent);

            logger.info(output_name + ", " + legends[i].getTypeName() + ": " + String.valueOf(e2));
            if (firstTime || e2 <= minE) {
                minE = e2;
                minI = i;
                firstTime = false;
            }
        }

        try {
            if (cutpointFile != null) {
                cutpointFile.append(filename).append(",").append(legends[minI].getTypeName());
                float[] minmax = legends[minI].getMinMax();
                float[] f = legends[minI].getCutoffFloats();

                cutpointFile.append(",min,").append(String.valueOf(minmax[0]));

                cutpointFile.append(",#cutpoints,").append(String.valueOf(f.length));

                cutpointFile.append(",cutpoints");
                for (int i = 0; i < f.length; i++) {
                    cutpointFile.append(",").append(String.valueOf(f[i]));
                }

                cutpointFile.append(",distribution");
                int[] a;
                if (useAreaEvaluation) {
                    a = legends[minI].groupSizesArea;
                } else {
                    a = legends[minI].groupSizes;
                }
                for (int i = 0; i < a.length; i++) {
                    cutpointFile.append(",").append(String.valueOf(a[i]));
                }

                cutpointFile.append("\n");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    static public void main(String[] args) {

        if (args.length == 0) {
            logger.info("args[0]=grid file without .grd or .gri\n"
                    + "args[1]=output prefix for +_cutpoints.csv +.jpg +_legend.txt\n"
                    + "args[2]=thumbnail scale down factor (optional) e.g. 1 (default), 2, 4, 8, 16 (16x16 times smaller)\n"
                    + "args[3]=min as transparent (optional) e.g. 0=false (default), 1=true");
            return;
        }

        String gridfilename = args[0];
        String outputfilename = null;
        if (args.length < 2) {
            outputfilename = args[0];
        } else {
            outputfilename = args[1];
        }

        int scaleDown = 1;
        if (args.length >= 3) {
            scaleDown = Integer.parseInt(args[2]);
        }
        boolean minAsTransparent = false;
        if (args.length >= 4) {
            minAsTransparent = args[3].equals("1");
        }

        generateGridLegend(gridfilename, outputfilename, scaleDown, minAsTransparent);
    }

    public static boolean generateGridLegend(String gridfilename, String outputfilename, int scaleDown, boolean minAsTransparent) {
        boolean ret = true;

        String[] legendTypes = {"Equal Area"};
        //String [] legendTypes = {"Even Interval","Even Interval Log 10","Equal Size","Equal Area"};

        FileWriter fw = null;
        try {
            fw = new FileWriter(outputfilename + "_cutpoints.csv");

            new GridLegend(
                    gridfilename,
                    outputfilename,
                    true,
                    legendTypes, fw, scaleDown, minAsTransparent);
            fw.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            ret = false;
        } finally {
            if (fw != null) {
                try {
                    fw.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return ret;
    }
}

