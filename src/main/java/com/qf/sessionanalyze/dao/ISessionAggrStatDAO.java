package com.qf.sessionanalyze.dao;

import com.qf.sessionanalyze.domain.SessionAggrStat;

/**
 * session聚合统计模块DAO接口
 *
 * @author Administrator
 */
public interface ISessionAggrStatDAO {

    /**
     * 插入session聚合统计结果
     *
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);

}
