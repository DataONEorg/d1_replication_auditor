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

import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.cn.log.AuditEvent;
import org.dataone.cn.log.AuditLogClientFactory;
import org.dataone.cn.log.AuditLogEntry;
import org.dataone.service.cn.replication.v1.ReplicationFactory;
import org.dataone.service.cn.replication.v1.ReplicationService;
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
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.ChecksumUtil;

import com.hazelcast.core.IMap;

/**
 * Strategy for auditing coordinating node replica copies of an object cataloged and
 * replicated by dataone.  Attempts to get the object bytes from each CN node found 
 * in the hzNodes map.  Then calculates the checksum of the object and compares to
 * the value found in system metadata.   If the checksum matches, the replica is considered
 * valid and the verified date of the replica record is updated.  However if the object
 * is not found or has a different checksum, the object needs to be replaced on that
 * node.  
 * 
 * @author sroseboo
 *
 */
public class CoordinatingNodeReplicaAuditingStrategy implements ReplicaAuditStrategy {

    public static Log log = LogFactory.getLog(CoordinatingNodeReplicaAuditingStrategy.class);

    private ReplicaAuditingDelegate auditDelegate = new ReplicaAuditingDelegate();
    private Map<NodeReference, CNode> cnMap = new HashMap<NodeReference, CNode>();

    private ReplicationService replicationService = ReplicationFactory.getReplicationService();
    private IMap<NodeReference, Node> hzNodes;

    public CoordinatingNodeReplicaAuditingStrategy() {
    }

    public void auditPids(List<Identifier> pids, Date auditDate) {
        for (Identifier pid : pids) {
            this.auditPid(pid, auditDate);
        }
    }

    private void auditPid(Identifier pid, Date auditDate) {

        // remove log entry if audit replica failed exists
        AuditLogClientFactory.getAuditLogClient().removeReplicaAuditEvent(
                new AuditLogEntry(pid.getValue(), auditDelegate.getCnRouterId(),
                        AuditEvent.REPLICA_AUDIT_FAILED, null, null));

        SystemMetadata sysMeta = auditDelegate.getSystemMetadata(pid);
        if (sysMeta == null) {
            return;
        }
        for (Replica replica : sysMeta.getReplicaList()) {
            if (auditDelegate.isCNodeReplica(replica)) {
                auditCNodeReplicas(sysMeta, replica);
                break;
            }
        }
        return;
    }

    private void auditCNodeReplicas(SystemMetadata sysMeta, Replica replica) {

        for (NodeReference nodeRef : hzNodes.keySet()) {
            Node node = hzNodes.get(nodeRef);
            if (NodeType.CN.equals(node.getType())
                    && auditDelegate.getCnRouterId().equals(node.getIdentifier().getValue()) == false) {
                CNode cn = getCNode(node);
                if (cn != null) {
                    Checksum expected = sysMeta.getChecksum();
                    Checksum actual = null;
                    boolean thisValid = true;
                    try {
                        actual = calculateCNChecksum(cn, sysMeta);
                    } catch (NotFound e) {
                        thisValid = false;
                        String message = "Attempt to retrieve the pid: "
                                + sysMeta.getIdentifier().getValue() + " from CN: "
                                + nodeRef.getValue() + " resulted in a D1 NotFound exception: "
                                + e.getMessage() + ".   Replica has NOT been marked invalid.";
                        log.error(message, e);
                        logReplicaAuditFailure(sysMeta.getIdentifier().getValue(), node
                                .getIdentifier().getValue(), AuditEvent.REPLICA_NOT_FOUND, message);
                    } catch (NotAuthorized e) {
                        thisValid = false;
                        String message = "Attempt to retrieve pid: "
                                + sysMeta.getIdentifier().getValue() + " from CN: "
                                + nodeRef.getValue()
                                + " resulted in a D1 NotAuthorized exception: " + e.getMessage()
                                + ".   Replica has NOT been marked invalid.";
                        log.error(message, e);
                        logReplicaAuditFailure(sysMeta.getIdentifier().getValue(), node
                                .getIdentifier().getValue(), AuditEvent.REPLICA_AUDIT_FAILED,
                                message);
                    } catch (NotImplemented e) {
                        thisValid = false;
                        String message = "Attempt to retrieve pid: "
                                + sysMeta.getIdentifier().getValue() + " from CN: "
                                + nodeRef.getValue()
                                + " resulted in a D1 NotImplemented exception: " + e.getMessage()
                                + ".   Replica has NOT been marked invalid.";
                        log.error(message, e);
                        logReplicaAuditFailure(sysMeta.getIdentifier().getValue(), node
                                .getIdentifier().getValue(), AuditEvent.REPLICA_AUDIT_FAILED,
                                message);
                    } catch (InvalidToken e) {
                        thisValid = false;
                        String message = "Attempt to retrieve pid: "
                                + sysMeta.getIdentifier().getValue() + " from CN: "
                                + nodeRef.getValue() + " resulted in a D1 InvalidToken exception: "
                                + e.getMessage() + ".   Replica has NOT been marked invalid.";
                        log.error(message, e);
                        logReplicaAuditFailure(sysMeta.getIdentifier().getValue(), node
                                .getIdentifier().getValue(), AuditEvent.REPLICA_AUDIT_FAILED,
                                message);
                    } catch (ServiceFailure e) {
                        thisValid = false;
                        String message = "Attempt to retrieve pid: "
                                + sysMeta.getIdentifier().getValue() + " from CN: "
                                + nodeRef.getValue()
                                + " resulted in a D1 ServiceFailure exception: " + e.getMessage()
                                + ".   Replica has NOT been marked invalid.";
                        log.error(message, e);
                        logReplicaAuditFailure(sysMeta.getIdentifier().getValue(), node
                                .getIdentifier().getValue(), AuditEvent.REPLICA_AUDIT_FAILED,
                                message);
                    }
                    if (actual != null && thisValid) {
                        thisValid = ChecksumUtil.areChecksumsEqual(expected, actual);
                    }

                    if (thisValid) {
                        AuditLogClientFactory.getAuditLogClient().removeReplicaAuditEvent(
                                new AuditLogEntry(sysMeta.getIdentifier().getValue(), nodeRef
                                        .getValue(), null, null, null));
                    } else {
                        String message = "Checksum mismatch for pid: "
                                + sysMeta.getIdentifier().getValue() + " against CN: "
                                + nodeRef.getValue() + ".  Expected checksum is: "
                                + expected.getValue() + " actual was: " + actual.getValue()
                                + ".  Replica has NOT been marked invalid.";
                        log.error(message);
                        logReplicaAuditFailure(sysMeta.getIdentifier().getValue(),
                                nodeRef.getValue(), AuditEvent.REPLICA_BAD_CHECKSUM, message);
                        //TODO: Handle an invalid CN replica copy.  
                        //      Request re-sync?  Set replica invalid?

                    }
                }
            }
        }
        updateReplicaVerified(sysMeta.getIdentifier(), replica);
    }

    private void logReplicaAuditFailure(String pid, String nodeId, AuditEvent event, String message) {
        AuditLogClientFactory.getAuditLogClient().removeReplicaAuditEvent(
                new AuditLogEntry(pid, nodeId, event, null, null));
        AuditLogEntry logEntry = new AuditLogEntry(pid, nodeId, event, message);
        AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
    }

    private void updateReplicaVerified(Identifier pid, Replica replica) {
        auditDelegate.updateVerifiedReplica(pid, replica);
    }

    private Checksum calculateCNChecksum(CNode cn, SystemMetadata sysmeta) throws NotFound,
            InvalidToken, ServiceFailure, NotAuthorized, NotImplemented {
        Checksum checksum = null;
        String algorithm = sysmeta.getChecksum().getAlgorithm();
        InputStream is = cn.get(sysmeta.getIdentifier());
        if (is != null) {
            try {
                checksum = ChecksumUtil.checksum(is, algorithm);
            } catch (Exception e) {
                log.error("Cannot calculate CN checksum for id: "
                        + sysmeta.getIdentifier().getValue(), e);
            }
        } else {
            log.error("Could not calculate checksum on CN, unable to get object bytes");
        }
        return checksum;
    }

    private CNode getCNode(Node node) {
        if (!cnMap.containsKey(node.getIdentifier().getValue())) {
            CNode cn = new CNode(node.getBaseURL());
            cn.setNodeId(node.getIdentifier().getValue());
            cnMap.put(node.getIdentifier(), cn);
        }
        return cnMap.get(node.getIdentifier());
    }
}
