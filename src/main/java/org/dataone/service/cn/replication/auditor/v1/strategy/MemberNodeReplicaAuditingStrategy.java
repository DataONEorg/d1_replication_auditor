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
package org.dataone.service.cn.replication.auditor.v1.strategy;

import java.math.BigInteger;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.cn.data.repository.ReplicationTask;
import org.dataone.cn.data.repository.ReplicationTaskRepository;
import org.dataone.cn.log.AuditEvent;
import org.dataone.cn.log.AuditLogClientFactory;
import org.dataone.cn.log.AuditLogEntry;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.replication.ReplicationFactory;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v2.SystemMetadata;

/**
 * This type of audit verifies both that the replication policy is fufilled and
 * that each Member Node replica is still valid (by comparing checksum values).
 * 
 * The authoritative Member Node's 'replica' is not marked 'invalid' if it is 
 * found to be missing or have a different checksum value (than what is recorded
 * in the system metadata record).  All behavior regarding changing the replica
 * status only applies to non-authoritative member node replica objects.  However,
 * the replica audit log message will still be generated - indicating an issue with 
 * the authoritative member node's replica object.
 * 
 * Replicas found with invalid checksums have system metadata replica status
 * updated to INVALID. Pids with unfufilled replica policies are sent to
 * ReplicationManager.
 * 
 * Verified replicas have their verified date updated to reflect audit complete
 * date.
 * 
 * @author sroseboo
 * 
 */
public class MemberNodeReplicaAuditingStrategy implements ReplicaAuditStrategy {

    public static Logger log = Logger.getLogger(MemberNodeReplicaAuditingStrategy.class);

    private static final BigInteger auditSizeLimit = Settings.getConfiguration().getBigInteger(
            "dataone.mn.audit.size.limit", BigInteger.valueOf(1000000000));

    private ReplicaAuditingDelegate auditDelegate = new ReplicaAuditingDelegate();
    private ReplicationTaskRepository taskRepository = ReplicationFactory
            .getReplicationTaskRepository();

    public MemberNodeReplicaAuditingStrategy() {
    }

    public void auditPids(List<Identifier> pids, Date auditDate) {
        log.debug("audit pids called with " + pids.size() + ".");
        for (Identifier pid : pids) {
            this.auditPid(pid, auditDate);
        }
    }

    /**
     * Audit the replication policy of a pid:
     * 1.) Verify document exists on MN.
     * 2.) Verify each MN replica (checksum).
     * 3.) Verify the replication policy of the pid is fufilled.
     * 
     * @param pid
     * @param auditDate
     * @return
     */
    private void auditPid(Identifier pid, Date auditDate) {
        log.debug("auditPid for Member Node replica called for pid: " + pid.getValue());
        SystemMetadata sysMeta = auditDelegate.getSystemMetadata(pid);
        if (sysMeta == null) {
            return;
        }

        boolean queueToReplication = false;
        int validReplicaCount = 0;

        for (Replica replica : sysMeta.getReplicaList()) {
            // parts of the replica policy may have already been validated recently.
            // only verify replicas with stale replica verified date (verify).
            boolean verify = replica.getReplicaVerified().before(auditDate);

            if (auditDelegate.isCNodeReplica(replica)) {
                // CN replicas should not be appearing in this auditors data selection but
                // may appear coincidentally having both a stale CN and MN replica.
                continue;
            } else if (auditDelegate.isAuthoritativeMNReplica(sysMeta, replica)) {
                if (verify) {
                    boolean valid = auditAuthoritativeMNodeReplica(sysMeta, replica);
                    if (!valid) {
                        queueToReplication = true;
                    }
                }
            } else { // not a CN replica, not the authMN replica - a MN replica.
                boolean valid = false;
                if (verify) {
                    valid = auditMemberNodeReplica(sysMeta, replica);
                }
                if (valid || !verify) {
                    validReplicaCount++;
                } else if (!valid) {
                    queueToReplication = true;
                }
            }
        }
        if (shouldSendToReplication(queueToReplication, sysMeta, validReplicaCount)) {
            sendToReplication(pid);
        }
        return;
    }

    // Return value indicates if replica is valid.
    private boolean auditMemberNodeReplica(SystemMetadata sysMeta, Replica replica) {

        Identifier pid = sysMeta.getIdentifier();

        if (auditSizeLimit.compareTo(sysMeta.getSize()) < 0) {
            // file is larger than audit limit - dont audit, update audit date, log non audit.
            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica
                    .getReplicaMemberNode().getValue(), AuditEvent.REPLICA_AUDIT_FAILED,
                    "replica audit skipped, size of document exceeds audit limit");
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
            updateReplicaVerified(pid, replica);
            return true;
        }

        Checksum expected = sysMeta.getChecksum();
        Checksum actual = null;

        try {
            actual = auditDelegate.getChecksumFromMN(pid, sysMeta, replica.getReplicaMemberNode());
        } catch (NotFound e) {
            log.error(e);
            String message = "Attempt to retrieve the checksum from source member node resulted in a D1 NotFound exception: "
                    + e.getMessage() + ".   Replica has been marked invalid.";

            handleInvalidReplica(sysMeta, replica);

            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica
                    .getReplicaMemberNode().getValue(), AuditEvent.REPLICA_NOT_FOUND, message);
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
            return false;
        } catch (BaseException e) {
            log.error("Unable to get checksum from mn: " + replica.getReplicaMemberNode() + ". ", e);
            updateReplicaVerified(pid, replica);
            String message = "Attempt to retrieve checksum from MN resulted in multiple ServiceFailure exceptions: "
                    + e.getMessage() + ".  Not invalidating replica.";
            logAuditingFailure(replica, pid, message);
            return true;
        }

        if (actual == null) {
            String message = "Attempt to retrieve the checksum from source member node resulted "
                    + "in a null checksum.  Replica has been marked invalid.";
            log.error(message);

            handleInvalidReplica(sysMeta, replica);

            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica
                    .getReplicaMemberNode().getValue(), AuditEvent.REPLICA_BAD_CHECKSUM, message);
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
            return false;
        }

        boolean valid = ChecksumUtil.areChecksumsEqual(actual, expected);
        if (valid) {
            updateReplicaVerified(pid, replica);
        } else {
            String message = "Checksum mismatch for pid: " + pid.getValue() + " against MN: "
                    + replica.getReplicaMemberNode().getValue() + ".  Expected checksum is: "
                    + expected.getValue() + " actual was: " + actual.getValue();
            log.error(message);

            handleInvalidReplica(sysMeta, replica);

            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica
                    .getReplicaMemberNode().getValue(), AuditEvent.REPLICA_BAD_CHECKSUM, message);
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
            return false;
        }
        return true;
    }

    // could not get the checksum for service failure, invalid request, invalid token reasons.
    // remove previous log entry event for this pid/node/event type since it going to repeat
    // until checksum can be generated.
    private void logAuditingFailure(Replica replica, Identifier pid, String message) {
        AuditLogClientFactory.getAuditLogClient().removeReplicaAuditEvent(
                new AuditLogEntry(pid.getValue(), replica.getReplicaMemberNode().getValue(),
                        AuditEvent.REPLICA_AUDIT_FAILED, null, null));
        AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica.getReplicaMemberNode()
                .getValue(), AuditEvent.REPLICA_AUDIT_FAILED, message);
        AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
    }

    private boolean auditAuthoritativeMNodeReplica(SystemMetadata sysMeta, Replica replica) {
        return auditMemberNodeReplica(sysMeta, replica);
    }

    private boolean shouldSendToReplication(boolean queueToReplication, SystemMetadata sysMeta,
            int validReplicaCount) {
        if (sysMeta.getReplicationPolicy() == null
                || sysMeta.getReplicationPolicy().getNumberReplicas() == null) {
            return false;
        }
        return queueToReplication
                || validReplicaCount != sysMeta.getReplicationPolicy().getNumberReplicas()
                        .intValue();
    }

    private void updateReplicaVerified(Identifier pid, Replica replica) {
        auditDelegate.updateVerifiedReplica(pid, replica);
    }

    /**
     * Set replica status invalid even if the authoritative MN copy.
     * 
     * @param pid
     * @param replica
     * @param authoritativeMN
     */
    private void handleInvalidReplica(SystemMetadata sysMeta, Replica replica) {
        auditDelegate.updateInvalidReplica(sysMeta, replica);
    }

    private void sendToReplication(Identifier pid) {
        List<ReplicationTask> taskList = taskRepository.findByPid(pid.getValue());
        if (taskList.size() == 1) {
            ReplicationTask task = taskList.get(0);
            task.markNew();
            taskRepository.save(task);
        } else if (taskList.size() == 0) {
            log.warn("In Replication Manager, task that should exist 'in process' does not exist.  Creating new task for pid: "
                    + pid.getValue());
            taskRepository.save(new ReplicationTask(pid));
        } else if (taskList.size() > 1) {
            log.warn("In Replication Manager, more than one task found for pid: " + pid.getValue()
                    + ". Deleting all and creating new task.");
            taskRepository.delete(taskList);
            taskRepository.save(new ReplicationTask(pid));
        }
    }
}
