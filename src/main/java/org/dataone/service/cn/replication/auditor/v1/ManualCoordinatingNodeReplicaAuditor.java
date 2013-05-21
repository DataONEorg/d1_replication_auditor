package org.dataone.service.cn.replication.auditor.v1;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.service.types.v1.Identifier;

public class ManualCoordinatingNodeReplicaAuditor extends AbstractReplicationAuditor {

    private static final int pageSize = 200;
    private static final int pidsPerTaskSize = 20;
    private static final int taskPoolSize = 10;
    private static final int maxPages = 100000;
    private static final String MANUAL_AUDIT_LOCK_NAME = "manualCoordinatingNodeReplicationAuditLock";
    private Date auditDate = null;

    public ManualCoordinatingNodeReplicaAuditor(Date auditDate) {
        this.auditDate = auditDate;
    }

    protected String getLockName() {
        return MANUAL_AUDIT_LOCK_NAME;
    }

    protected Date calculateAuditDate() {
        return auditDate;
    }

    protected List<Identifier> getPidsToAudit(Date auditDate, int pageNumber, int pageSize)
            throws DataAccessException {
        return this.replicationDao.getCompletedCoordinatingNodeReplicasByDate(auditDate,
                pageNumber, pageSize);
    }

    protected Callable<String> newAuditTask(List<Identifier> pids, Date auditDate) {
        return new CoordinatingNodeReplicaAuditTask(pids, auditDate);
    }

    @Override
    protected boolean tryLock(Lock lock) {
        return true;
    }

    @Override
    protected void releaseLock(Lock lock) {
    }

    @Override
    protected Lock getProcessingLock() {
        return null;
    }

    protected int getMaxPages() {
        return maxPages;
    }

    protected int getTaskPoolSize() {
        return taskPoolSize;
    }

    protected int getPageSize() {
        return pageSize;
    }

    protected int getPidsPerTaskSize() {
        return pidsPerTaskSize;
    }

    protected boolean shouldRunAudit() {
        return true;
    }

}
