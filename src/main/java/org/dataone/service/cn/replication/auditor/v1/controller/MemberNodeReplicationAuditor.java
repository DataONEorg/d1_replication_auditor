/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dataone.service.cn.replication.auditor.v1.controller;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.replication.auditor.v1.task.MemberNodeReplicaAuditTask;
import org.dataone.service.types.v1.Identifier;

/**
 * Concrete implementation of AbstractReplicationAuditor to provide specific
 * behavior for auditing member node replicas using callable auditing tasks.
 * This class provides specific implementation for methods to select pids for
 * auditing, creating member node auditing tasks, controlling the audit period,
 * and configuration of the executor service.
 * 
 * @author sroseboo
 *
 */
public class MemberNodeReplicationAuditor extends AbstractReplicationAuditor {

    private static final int pageSize = 100;
    private static final int pidsPerTaskSize = 10;
    private static final int taskPoolSize = 10;
    private static final int maxPages = 50;

    private static final int auditPeriodDays = Settings.getConfiguration().getInt(
            "Replication.audit.mn.period.days", 90);

    private static final long auditPeriod = 1000 * 60 * 60 * 24 * auditPeriodDays;

    private static final String MN_AUDIT_LOCK_NAME = "memberNodeReplicationAuditLock";

    @Override
    protected String getLockName() {
        return MN_AUDIT_LOCK_NAME;
    }

    @Override
    protected Date calculateAuditDate() {
        return new Date(System.currentTimeMillis() - auditPeriod);
    }

    @Override
    protected List<Identifier> getPidsToAudit(Date auditDate, int pageNumber, int pageSize)
            throws DataAccessException {
        return this.replicationDao.getCompletedMemberNodeReplicasByDate(auditDate, pageNumber,
                pageSize);
    }

    @Override
    protected Callable<String> newAuditTask(List<Identifier> pids, Date auditDate) {
        return new MemberNodeReplicaAuditTask(pids, auditDate);
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

    @Override
    protected boolean shouldRunAudit() {
        return ComponentActivationUtility.replicationMNAuditorIsActive();
    }
}
