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
package org.dataone.service.cn.replication.auditor.v1.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.dataone.service.cn.replication.auditor.v1.strategy.MemberNodeReplicaAuditingStrategy;
import org.dataone.service.cn.replication.auditor.v1.strategy.ReplicaAuditStrategy;
import org.dataone.service.types.v1.Identifier;

/**
 * Callable java task, delegates to MemberNodeReplicaAuditingStrategy to handle
 * audit work for each pid in pidsToAudit.
 * 
 * @author sroseboo
 *
 */
public class MemberNodeReplicaAuditTask implements Serializable, Callable<String> {

    private static final long serialVersionUID = 8549092026722882706L;
    private static Logger log = Logger.getLogger(MemberNodeReplicaAuditTask.class.getName());

    private List<Identifier> pidsToAudit = new ArrayList<Identifier>();
    private ReplicaAuditStrategy auditor;
    private Date auditDate;

    public MemberNodeReplicaAuditTask(List<Identifier> pids, Date auditDate) {
        this.pidsToAudit.addAll(pids);
        log.debug("audit task has " + pids.size() + " pids to audit.");
        this.auditDate = auditDate;
        auditor = new MemberNodeReplicaAuditingStrategy();
    }

    @Override
    public String call() throws Exception {
        auditor.auditPids(pidsToAudit, auditDate);
        return "Member Node replica audit task for pids: " + pidsToAudit.size() + " completed.";
    }

    public List<Identifier> getPidsToAudit() {
        return pidsToAudit;
    }
}
