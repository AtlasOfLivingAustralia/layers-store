/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package au.org.ala.layers.util;

import au.org.ala.layers.intersect.Grid;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * @author Adam
 */
public class FixGridMinMax {

    private static Logger logger = Logger.getLogger(FixGridMinMax.class);

    static public void main(String[] args) {
        logger.info("args[0] = directory containing diva grid files.");
        logger.info("args[1] = directory for new headers.");

        if (args.length > 1) {
            for (File f : new File(args[0]).listFiles()) {
                if (f.getName().endsWith(".grd")) {
                    String name = f.getName();
                    Grid g = new Grid(f.getParent() + File.separator + name.substring(0, name.length() - 4));
                    float[] minmax = g.calculatetMinMax();
                    if (g.minval != minmax[0] || g.maxval != minmax[1]) {
                        logger.info(f.getName() + "," + g.nodatavalue
                                + "," + g.minval + "," + g.maxval + "," + minmax[0] + "," + minmax[1]);
                        g.writeHeader(args[1] + name.substring(0, name.length() - 4), g.xmin, g.ymin, g.xmax, g.ymax, g.xres, g.yres, g.nrows, g.ncols, minmax[0], minmax[1], g.datatype, String.valueOf(g.nodatavalue));
                    }
                }
            }
        }
    }
}
