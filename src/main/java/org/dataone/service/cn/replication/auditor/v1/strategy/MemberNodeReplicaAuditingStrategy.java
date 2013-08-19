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

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.MNode;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.log.AuditEvent;
import org.dataone.cn.log.AuditLogClientFactory;
import org.dataone.cn.log.AuditLogEntry;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.ChecksumUtil;

import com.hazelcast.core.IQueue;

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

    public static Log log = LogFactory.getLog(MemberNodeReplicaAuditingStrategy.class);
    private ReplicaAuditingDelegate auditDelegate = new ReplicaAuditingDelegate();

    private static final String replicationEventQueueName = Settings.getConfiguration().getString(
            "dataone.hazelcast.replicationQueuedEvents");

    private IQueue<Identifier> replicationEvents = HazelcastClientFactory.getProcessingClient()
            .getQueue(replicationEventQueueName);

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
                    valid = auditMemberNodeReplica(sysMeta, replica, false);
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

    private boolean auditMemberNodeReplica(SystemMetadata sysMeta, Replica replica,
            boolean authoritativeMN) {

        MNode mn = auditDelegate.getMNode(replica.getReplicaMemberNode());
        if (mn == null) {
            return true;
        }

        Identifier pid = sysMeta.getIdentifier();
        Checksum expected = sysMeta.getChecksum();
        Checksum actual = null;

        try {
            actual = auditDelegate.getChecksumFromMN(pid, sysMeta, mn);
        } catch (NotFound e) {
            handleInvalidReplica(pid, replica, authoritativeMN);
            String message = "Attempt to retrieve the checksum from source member node resulted in a D1 NotFound exception: "
                    + e.getMessage() + ".   Replica has been marked invalid.";
            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica
                    .getReplicaMemberNode().getValue(), AuditEvent.REPLICA_NOT_FOUND, message);
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
            return false;
        } catch (ServiceFailure e) {
            log.error("Unable to get checksum from mn: " + mn.getNodeId() + ". ", e);
            String message = "Attempt to retrieve checksum from MN resulted in multiple ServiceFailure exceptions: "
                    + e.getMessage()
                    + ".  Not invalidating replica, will attempt to audit again.  "
                    + "Only the current occurance of this error will remain in the log.";
            logAuditingFailure(replica, pid, message);
            return true;
        } catch (InvalidRequest e) {
            log.error("Unable to get checksum from mn: " + mn.getNodeId() + ". ", e);
            String message = "Attempt to retrieve checksum from MN resulted in multiple InvalidRequest exceptions: "
                    + e.getMessage()
                    + ".  Not invalidating replica, will attempt to audit again.  "
                    + "Only the current occurance of this error will remain in the log.";
            logAuditingFailure(replica, pid, message);
            return true;
        } catch (InvalidToken e) {
            log.error("Unable to get checksum from mn: " + mn.getNodeId() + ". ", e);
            String message = "Attempt to retrieve checksum from MN resulted in multiple InvalidToken exceptions: "
                    + e.getMessage()
                    + ".  Not invalidating replica, will attempt to audit again.  "
                    + "Only the current occurance of this error will remain in the log.";
            logAuditingFailure(replica, pid, message);
            return true;
        }

        if (actual == null) {
            String message = "Attempt to retrieve the checksum from source member node resulted "
                    + "in a null checksum.  Not invalidating replica, will attempt to audit again.  "
                    + "Only the current occurance of this error will remain in the log.";
            logAuditingFailure(replica, pid, message);
            return true;
        }

        boolean valid = ChecksumUtil.areChecksumsEqual(actual, expected);
        if (valid) {
            updateReplicaVerified(pid, replica);
        } else {
            String message = "Checksum mismatch for pid: " + pid.getValue() + " against MN: "
                    + replica.getReplicaMemberNode().getValue() + ".  Expected checksum is: "
                    + expected.getValue() + " actual was: " + actual.getValue();
            log.error(message);
            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica
                    .getReplicaMemberNode().getValue(), AuditEvent.REPLICA_BAD_CHECKSUM, message);
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
            handleInvalidReplica(pid, replica, authoritativeMN);
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
        boolean verified = auditMemberNodeReplica(sysMeta, replica, true);
        if (!verified) {
            //any further special processing for auth MN?
        }
        return verified;
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
     * Only set replica status invalid if not the authoritative MN copy.
     * 
     * @param pid
     * @param replica
     * @param authoritativeMN
     */
    private void handleInvalidReplica(Identifier pid, Replica replica, boolean authoritativeMN) {
        if (authoritativeMN == false) {
            auditDelegate.updateInvalidReplica(pid, replica);
        }
    }

    private void sendToReplication(Identifier pid) {
        try {
            log.debug("Sending pid to replication event queue: " + pid.getValue());
            replicationEvents.add(pid);
        } catch (Exception e) {
            log.error("Pid: " + pid + " not accepted by replicationManager createAndQueueTasks: ",
                    e);
        }
    }
}
