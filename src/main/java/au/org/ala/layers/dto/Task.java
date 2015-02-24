/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 *
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.layers.dto;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.persistence.*;
import java.util.Date;

/**
 * This class serves as a model object for the "fields" table
 *
 * @author ajay
 */
@Entity
@Table(name = "fields")
@JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
public class Task {

    @Id
    @Column(name = "id", insertable = false, updatable = false)
    private Integer id;
    @Column(name = "name")
    private String name;
    @Column(name = "json")
    private String json;
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "created")
    private Date created;
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "started")
    private Date started;
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "finished")
    private Date finished;
    @Column(name = "error")
    private String error;
    @Column(name = "size")
    private Integer size;

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

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getFinished() {
        return finished;
    }

    public void setFinished(Date finished) {
        this.finished = finished;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Date getStarted() {
        return started;
    }

    public void setStarted(Date started) {
        this.started = started;
    }
}
