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
package org.dataone.service.cn.replication.auditor.v1;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.MNode;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.replication.v1.ReplicationFactory;
import org.dataone.service.cn.replication.v1.ReplicationService;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.ChecksumUtil;

import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;

/**
 * This type of audit verifies both that the replication policy is fufilled and
 * that each Member Node replica is still valid (by comparing checksum values).
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
public class MemberNodeReplicaAuditingStrategy {

    public static Log log = LogFactory.getLog(MemberNodeReplicaAuditingStrategy.class);
    private String replicationEventQueueName = Settings.getConfiguration().getString(
            "dataone.hazelcast.replicationQueuedEvents");

    private ReplicationService replicationService;
    private IMap<NodeReference, Node> hzNodes;
    private IQueue<Identifier> replicationEvents;

    private Map<NodeReference, MNode> mnMap = new HashMap<NodeReference, MNode>();

    public MemberNodeReplicaAuditingStrategy() {
        replicationService = ReplicationFactory.getReplicationService();
        replicationEvents = HazelcastClientFactory.getProcessingClient().getQueue(
                replicationEventQueueName);
        hzNodes = HazelcastClientFactory.getProcessingClient().getMap("hzNodes");
    }

    public void auditPids(List<Identifier> pids, Date auditDate) {
        log.debug("audit pids called with " + pids.size() + ".");
        for (Identifier pid : pids) {
            this.auditPid(pid, auditDate);
        }
    }

    /**
     * 
     * Audit the replication policy of a pid:
     * 2.) Verify each MN replica (checksum).
     * 3.) Verify the replication policy of the pid is fufilled.
     * 
     * 
     * @param pid
     * @param auditDate
     * @return
     */
    public void auditPid(Identifier pid, Date auditDate) {
        log.debug("audit pid called for pid: " + pid.getValue());
        SystemMetadata sysMeta = null;
        try {
            sysMeta = replicationService.getSystemMetadata(pid);
        } catch (NotFound e) {
            log.error("Could not find system meta for pid: " + pid.getValue());
        }
        if (sysMeta == null) {
            log.error("Cannot get system metadata from CN for pid: " + pid
                    + ".  Could not audit replicas for pid: " + pid + "");
            return;
        }

        boolean queueToReplication = false;
        int validReplicaCount = 0;

        for (Replica replica : sysMeta.getReplicaList()) {
            // parts of the replica policy may have already been validated recently.
            boolean verify = replica.getReplicaVerified().before(auditDate);

            // only verify replicas with stale replica verified date (verify).
            if (isCNodeReplica(replica)) {
                // CN replicas should not be appearing in this auditors data selection but
                // may appear coincidentally having both a stale CN and MN replica.
                continue;
            } else if (isAuthoritativeMNReplica(sysMeta, replica)) {
                if (verify) {
                    boolean verified = auditAuthoritativeMNodeReplica(sysMeta, replica);
                    if (!verified) {
                        queueToReplication = true;
                    }
                }
            } else { // not a CN replica, not the authMN replica - a MN replica!
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

    private boolean auditMemberNodeReplica(SystemMetadata sysMeta, Replica replica) {

        MNode mn = getMNode(replica.getReplicaMemberNode());
        if (mn == null) {
            return true;
        }

        Identifier pid = sysMeta.getIdentifier();
        Checksum expected = sysMeta.getChecksum();
        Checksum actual = null;
        boolean valid = true;

        try {
            actual = getChecksumFromMN(pid, sysMeta, mn);
        } catch (NotFound e) {
            valid = false;
        }

        if (actual != null && valid) {
            valid = ChecksumUtil.areChecksumsEqual(actual, expected);
        }
        if (valid) {
            updateReplicaVerified(pid, replica);
        } else {
            log.error("Checksum mismatch for pid: " + pid.getValue() + " against MN: "
                    + replica.getReplicaMemberNode().getValue() + ".  Expected checksum is: "
                    + expected.getValue() + " actual was: " + actual.getValue());
            handleInvalidReplica(pid, replica);
        }
        return valid;
    }

    private boolean auditAuthoritativeMNodeReplica(SystemMetadata sysMeta, Replica replica) {
        boolean verified = auditMemberNodeReplica(sysMeta, replica);
        if (!verified) {
            //any further special processing for auth MN?
        }
        return verified;
    }

    private void updateReplicaVerified(Identifier pid, Replica replica) {
        replica.setReplicaVerified(calculateReplicaVerifiedDate());
        boolean success = replicationService.updateReplicationMetadata(pid, replica);
        if (!success) {
            log.error("Cannot update replica verified date  for pid: " + pid + " on CN");
        }
    }

    private Date calculateReplicaVerifiedDate() {
        return new Date(System.currentTimeMillis());
    }

    private boolean isCNodeReplica(Replica replica) {
        return NodeType.CN.equals(hzNodes.get(replica.getReplicaMemberNode()).getType());
    }

    private boolean isAuthoritativeMNReplica(SystemMetadata sysMeta, Replica replica) {
        return replica.getReplicaMemberNode().getValue()
                .equals(sysMeta.getAuthoritativeMemberNode().getValue());
    }

    private boolean shouldSendToReplication(boolean queueToReplication, SystemMetadata sysMeta,
            int validReplicaCount) {
        return queueToReplication
                || validReplicaCount != sysMeta.getReplicationPolicy().getNumberReplicas()
                        .intValue();
    }

    private void handleInvalidReplica(Identifier pid, Replica replica) {
        replica.setReplicationStatus(ReplicationStatus.INVALIDATED);
        boolean success = replicationService.updateReplicationMetadata(pid, replica);
        if (!success) {
            log.error("Cannot update replica status to INVALID for pid: " + pid + " on MN: "
                    + replica.getReplicaMemberNode().getValue());
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

    private Checksum getChecksumFromMN(Identifier pid, SystemMetadata sysMeta, MNode mn)
            throws NotFound {
        Checksum checksum = null;
        for (int i = 0; i < 5; i++) {
            try {
                checksum = mn.getChecksum(pid, sysMeta.getChecksum().getAlgorithm());
                break;
            } catch (ServiceFailure e) {
                // try again, no audit (skip)
            } catch (InvalidRequest e) {
                // try again, no audit (skip)
            } catch (InvalidToken e) {
                // try again, no audit (skip)
            } catch (NotAuthorized e) {
                // cannot audit, set to invalid
                checksum = new Checksum();
                break;
            } catch (NotImplemented e) {
                // cannot audit, set to invalid
                checksum = new Checksum();
                break;
            }
        }
        return checksum;
    }

    private MNode getMNode(NodeReference nodeRef) {
        if (!mnMap.containsKey(nodeRef)) {
            MNode mn = replicationService.getMemberNode(nodeRef);
            if (mn != null) {
                try {
                    mn.ping();
                    mnMap.put(nodeRef, mn);
                } catch (BaseException e) {
                    log.error("Unable to ping MN: " + nodeRef.getValue(), e);
                }
            } else {
                log.error("Cannot get MN: " + nodeRef.getValue()
                        + " unable to verify replica information.");
            }
        }
        return mnMap.get(nodeRef);
    }
}
