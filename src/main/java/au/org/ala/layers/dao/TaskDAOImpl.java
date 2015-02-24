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
package au.org.ala.layers.dao;

import au.org.ala.layers.dto.Task;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.simple.ParameterizedBeanPropertyRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ajay
 */
@Service("taskDao")
public class TaskDAOImpl implements TaskDAO {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(TaskDAOImpl.class);

    private SimpleJdbcTemplate jdbcTemplate;
    private SimpleJdbcInsert insertTask;

    @Resource(name = "dataSource")
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        this.insertTask = new SimpleJdbcInsert(dataSource).withTableName("task")
                .usingColumns("name", "json", "size");
    }

    public List<Task> getTasks(int page, int pageSize, int minSize, int maxSize, boolean includeFinished, boolean includeStarted) {

        String sql = "SELECT * FROM task " +
                "WHERE size >= " + minSize + " AND size <= " + maxSize +
                (!includeFinished ? " AND finished is NULL " : " ") +
                (!includeStarted ? " AND started is NULL " : " ") +
                " ORDER BY created " +
                " LIMIT " + pageSize + " OFFSET " + page;

        System.out.println(sql);

        return jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Task.class));
    }

    @Transactional
    public synchronized void addTask(String name, String json, Integer size) {
        if (jdbcTemplate.queryForInt("select count(*) from task where name = '" + name + "' and " +
                (json == null ? " json = '' " : " json = '" + json + "' ") +
                " and started is null ") == 0) {
            Map m = new HashMap();
            m.put("name", name);
            m.put("json", json);
            m.put("size", size);

            insertTask.execute(m);
        }
    }

    public synchronized boolean startTask(int id) {
        if (jdbcTemplate.queryForInt("select count(*) from task where id = " + id + " AND started is NULL") > 0) {
            String sql = "UPDATE task SET started = CURRENT_TIMESTAMP WHERE id = " + id + " AND started is NULL";
            jdbcTemplate.update(sql);
            return true;
        } else {
            return false;
        }
    }

    public void endTask(int id, String error) {
        if (error != null && error.length() > 0) {
            String sql = "UPDATE task SET finished = CURRENT_TIMESTAMP " +
                    ", error = ? " +
                    " WHERE id = " + id;
            jdbcTemplate.update(sql, error);
        } else {
            String sql = "UPDATE task SET finished = CURRENT_TIMESTAMP " +
                    " WHERE id = " + id;
            jdbcTemplate.update(sql);
        }


    }

    @Override
    public Task getNextTask(int maxSize) {
        List<Task> tasks = getTasks(0, 1, -1, maxSize, false, false);

        if (tasks.size() > 0) {
            return tasks.get(0);
        } else {
            return null;
        }
    }

    @Override
    public void resetStartedTasks() {
        String sql = "UPDATE task SET started = null WHERE started is not null AND finished is null";
        jdbcTemplate.update(sql);
    }

}
