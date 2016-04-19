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
package au.org.ala.layers.dto;

/**
 * @author Adam
 */
public class GridClass {

    Integer id;
    String name;
    Double area_km;
    String bbox;
    Integer minShapeIdx;
    Integer maxShapeIdx;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getArea_km() {
        return area_km;
    }

    public void setArea_km(Double area_km) {
        this.area_km = area_km;
    }

    public String getBbox() {
        return bbox;
    }

    public void setBbox(String bbox) {
        this.bbox = bbox;
    }

    public Integer getMinShapeIdx() {
        return minShapeIdx;
    }

    public void setMinShapeIdx(Integer minShapeIdx) {
        this.minShapeIdx = minShapeIdx;
    }

    public Integer getMaxShapeIdx() {
        return maxShapeIdx;
    }

    public void setMaxShapeIdx(Integer maxShapeIdx) {
        this.maxShapeIdx = maxShapeIdx;
    }
}
