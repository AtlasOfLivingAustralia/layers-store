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
package au.org.ala.layers.client;

import au.org.ala.layers.dao.*;
import au.org.ala.layers.dto.Field;
import au.org.ala.layers.dto.Layer;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Iterator;
import java.util.List;

/**
 * Main CLI class to hook into layers-store
 *
 * @author ajay and Qifeng
 */
public class Client {

    private static final Logger logger = Logger.getLogger(Client.class);

    static ApplicationContext gContext = null;

    public static void main(String[] args) {
        logger.info("Layers Store CLI client");

        //Test if ApplicationContext is autowired
        List<Layer> layers = Client.getLayerDao().getLayers();
        System.out.println(layers.size());
        List<Field> fields = Client.getFieldDao().getFields();
        System.out.println(fields.size());
    }

    public static void setContext(ApplicationContext context) {
        gContext = context;
    }

    static void initContext() {
        if (gContext == null) {
            Object obj = DefaultContextLocator.getApplicationContext();
            if (obj != null && obj instanceof ApplicationContext
                    && ((ApplicationContext) obj).getBean("layerDao") != null) {
                gContext = (ApplicationContext) obj;
            } else {
                gContext = new ClassPathXmlApplicationContext("spring/app-config.xml");
            }
        }
    }

    public static LayerDAO getLayerDao() {
        initContext();
        return (LayerDAO) gContext.getBean("layerDao");
    }

    public static LayerIntersectDAO getLayerIntersectDao() {
        initContext();
        LayerIntersectDAO lidao = (LayerIntersectDAO) gContext.getBean("layerIntersectDao");
        lidao.getConfig();  //also performs init.
        return lidao;
    }

    public static FieldDAO getFieldDao() {
        initContext();
        return (FieldDAO) gContext.getBean("fieldDao");
    }

    public static AnalysisLayerDAO getAnalysisLayerDao() {
        initContext();
        return (AnalysisLayerDAO) gContext.getBean("analysislayerDao");
    }

    public static ObjectDAO getObjectDao() {
        initContext();
        return (ObjectDAO) gContext.getBean("objectDao");
    }

    public static UserDataDAO getUserDataDao() {
        initContext();
        return (UserDataDAO) gContext.getBean("userDataDao");
    }
}

class DefaultContextLocator implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        System.out.println("Inside setApplicationContext");
        applicationContext = context;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}

