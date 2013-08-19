package org.dataone.service.cn.replication.auditor.v1.strategy;

import java.util.Date;
import java.util.List;

import org.dataone.service.types.v1.Identifier;

public interface ReplicaAuditStrategy {
    public void auditPids(List<Identifier> pids, Date auditDate);
}
