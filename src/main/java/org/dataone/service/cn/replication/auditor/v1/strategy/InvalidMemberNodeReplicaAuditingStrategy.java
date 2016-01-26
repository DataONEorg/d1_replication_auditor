package org.dataone.service.cn.replication.auditor.v1.strategy;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.cn.log.AuditEvent;
import org.dataone.cn.log.AuditLogClientFactory;
import org.dataone.cn.log.AuditLogEntry;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v2.SystemMetadata;

/**
 * Strategy for auditing invalid member node replica objects.
 * Optimistic audit - looking at invalid member node replicas
 * and checking to see if the replica has been 'repaired' such
 * that the replica is now present with expected checksum value.
 * 
 * Since neither CN replica nor authoritative MN replicas are marked
 * invalid by auditing, these replicas are currently ignored by this 
 * replication strategy.
 * 
 * @author sroseboo
 *
 */
public class InvalidMemberNodeReplicaAuditingStrategy implements ReplicaAuditStrategy {

    public static Logger log = Logger.getLogger(InvalidMemberNodeReplicaAuditingStrategy.class);
    private ReplicaAuditingDelegate auditDelegate = new ReplicaAuditingDelegate();

    public InvalidMemberNodeReplicaAuditingStrategy() {
    }

    public void auditPids(List<Identifier> pids, Date auditDate) {
        log.debug("audit pids called with " + pids.size() + ".");
        for (Identifier pid : pids) {
            auditPid(pid, auditDate);
        }
    }

    private void auditPid(Identifier pid, Date auditDate) {
        log.debug("auditPid for invalid replica called for pid: " + pid.getValue());
        SystemMetadata sysMeta = auditDelegate.getSystemMetadata(pid);
        if (sysMeta == null) {
            return;
        }
        for (Replica replica : sysMeta.getReplicaList()) {
            if (auditDelegate.isCNodeReplica(replica)) {
                // CN replicas should not be appearing in this auditors data selection but
                // may appear coincidentally having both a stale CN and MN replica.
                continue;
                // audit any MN replica even authMN copies
                //            } else if (auditDelegate.isAuthoritativeMNReplica(sysMeta, replica)) {
                //                continue;
            } else {
                // not a CN replica, not the authMN replica - a invalid MN replica.

                // parts of the replica policy may have already been validated recently.
                // only verify replicas with stale replica verified date (verify).
                boolean verify = replica.getReplicaVerified().before(auditDate);
                if (verify) {
                    auditInvalidMemberNodeReplica(sysMeta, replica);
                }
            }
        }
    }

    private void auditInvalidMemberNodeReplica(SystemMetadata sysMeta, Replica replica) {

        Identifier pid = sysMeta.getIdentifier();
        Checksum expected = sysMeta.getChecksum();
        Checksum actual = null;

        try {
            actual = auditDelegate.getChecksumFromMN(pid, sysMeta, replica.getReplicaMemberNode());
        } catch (BaseException e) {
            e.printStackTrace();
        }

        if (actual == null) {

        }

        boolean valid = ChecksumUtil.areChecksumsEqual(actual, expected);
        if (valid) {
            updateReplicaToComplete(pid, replica);
        } else {
            log.error("Checksum mismatch for pid: " + pid.getValue());
            log.error(" against MN: " + replica.getReplicaMemberNode().getValue() + ".");
            log.error("Expected checksum is: " + expected.getValue());
            String actualChecksum = null;
            if (actual != null) {
                actualChecksum = actual.getValue();
            }
            log.error(" actual was: " + actualChecksum);
            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica
                    .getReplicaMemberNode().getValue(), AuditEvent.REPLICA_BAD_CHECKSUM,
                    "Checksum mismatch for pid: " + pid.getValue() + " against MN: "
                            + replica.getReplicaMemberNode().getValue()
                            + ".  Expected checksum is: " + expected.getValue() + " actual was: "
                            + actualChecksum);
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
            handleInvalidReplica(sysMeta, replica);
        }
    }

    private void handleInvalidReplica(SystemMetadata sysMeta, Replica replica) {
        auditDelegate.updateInvalidReplica(sysMeta, replica);
    }

    private void updateReplicaToComplete(Identifier pid, Replica replica) {
        auditDelegate.updateVerifiedReplica(pid, replica);
    }
}
