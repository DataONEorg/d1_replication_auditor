package org.dataone.service.cn.replication.auditor.v1.controller;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.replication.auditor.v1.task.InvalidMemberNodeReplicaAuditTask;
import org.dataone.service.types.v1.Identifier;

public class InvalidMemberNodeReplicationAuditor extends AbstractReplicationAuditor {

    private static final int pageSize = 100;
    private static final int pidsPerTaskSize = 10;
    private static final int taskPoolSize = 10;
    private static final int maxPages = 50;

    private static final int auditPeriodDays = Settings.getConfiguration().getInt(
            "Replication.audit.mn.invalid.period.days", 10);

    private static final long auditPeriod = 1000 * 60 * 60 * 24 * auditPeriodDays;

    private static final String AUDIT_LOCK_NAME = "invalidMNReplicationAuditLock";

    @Override
    protected List<Identifier> getPidsToAudit(Date auditDate, int pageNumber, int pageSize)
            throws DataAccessException {
        return this.replicationDao.getInvalidMemberNodeReplicasByDate(auditDate, pageNumber,
                pageSize);
    }

    @Override
    protected Callable<String> newAuditTask(List<Identifier> pids, Date auditDate) {
        return new InvalidMemberNodeReplicaAuditTask(pids, auditDate);
    }

    @Override
    protected String getLockName() {
        return AUDIT_LOCK_NAME;
    }

    @Override
    protected Date calculateAuditDate() {
        return new Date(System.currentTimeMillis() - auditPeriod);
    }

    @Override
    protected int getMaxPages() {
        return maxPages;
    }

    @Override
    protected int getTaskPoolSize() {
        return taskPoolSize;
    }

    @Override
    protected int getPageSize() {
        return pageSize;
    }

    @Override
    protected int getPidsPerTaskSize() {
        return pidsPerTaskSize;
    }

    @Override
    protected boolean shouldRunAudit() {
        return ComponentActivationUtility.replicationMNAuditorIsActive();
    }
}
