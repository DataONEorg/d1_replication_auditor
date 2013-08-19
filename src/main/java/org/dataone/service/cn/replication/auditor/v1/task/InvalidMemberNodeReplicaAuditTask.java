package org.dataone.service.cn.replication.auditor.v1.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.dataone.service.cn.replication.auditor.v1.strategy.InvalidMemberNodeReplicaAuditingStrategy;
import org.dataone.service.cn.replication.auditor.v1.strategy.ReplicaAuditStrategy;
import org.dataone.service.types.v1.Identifier;

public class InvalidMemberNodeReplicaAuditTask implements Serializable, Callable<String> {

    private static final long serialVersionUID = -1774115683110659960L;
    private static Logger log = Logger.getLogger(InvalidMemberNodeReplicaAuditTask.class.getName());

    private List<Identifier> pidsToAudit = new ArrayList<Identifier>();
    private ReplicaAuditStrategy auditor;
    private Date auditDate;

    public InvalidMemberNodeReplicaAuditTask(List<Identifier> pids, Date auditDate) {
        this.pidsToAudit.addAll(pids);
        log.debug("audit task has " + pids.size() + " pids to audit.");
        this.auditDate = auditDate;
        auditor = new InvalidMemberNodeReplicaAuditingStrategy();
    }

    @Override
    public String call() throws Exception {
        auditor.auditPids(pidsToAudit, auditDate);
        return "Invalid member node replica audit task for pids: " + pidsToAudit.size()
                + " completed.";
    }

    public List<Identifier> getPidsToAudit() {
        return pidsToAudit;
    }
}
