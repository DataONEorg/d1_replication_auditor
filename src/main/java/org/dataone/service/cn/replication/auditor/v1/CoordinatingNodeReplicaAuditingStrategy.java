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

import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.configuration.Settings;
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
public class CoordinatingNodeReplicaAuditingStrategy {

    public static Log log = LogFactory.getLog(MemberNodeReplicaAuditingStrategy.class);

    private Map<NodeReference, CNode> cnMap = new HashMap<NodeReference, CNode>();

    private static final String cnRouterId = Settings.getConfiguration().getString(
            "cn.router.nodeId", "urn:node:CN");

    private ReplicationService replicationService;
    private IMap<NodeReference, Node> hzNodes;

    public CoordinatingNodeReplicaAuditingStrategy() {
        replicationService = ReplicationFactory.getReplicationService();
        hzNodes = HazelcastClientFactory.getProcessingClient().getMap("hzNodes");
    }

    public void auditPids(List<Identifier> pids, Date auditDate) {
        for (Identifier pid : pids) {
            this.auditPid(pid, auditDate);
        }
    }

    public void auditPid(Identifier pid, Date auditDate) {

        SystemMetadata sysMeta = null;
        try {
            sysMeta = replicationService.getSystemMetadata(pid);
        } catch (NotFound e) {

        }
        if (sysMeta == null) {
            log.error("Cannot get system metadata from CN for pid: " + pid
                    + ".  Could not audit CN replica for pid: " + pid + "");
            return;
        }

        for (Replica replica : sysMeta.getReplicaList()) {
            if (isCNodeReplica(replica)) {
                auditCNodeReplica(sysMeta, replica);
            } else {
                log.error("found MN replica in Coordinating Node auditing for pid: "
                        + pid.getValue());
            }
        }
        return;
    }

    private void auditCNodeReplica(SystemMetadata sysMeta, Replica replica) {

        NodeReference invalidCN = null;
        boolean valid = true;

        for (NodeReference nodeRef : hzNodes.keySet()) {
            Node node = hzNodes.get(nodeRef);
            if (NodeType.CN.equals(node.getType())
                    && cnRouterId.equals(node.getIdentifier().getValue()) == false) {

                CNode cn = getCNode(node);
                if (cn != null) {
                    Checksum expected = sysMeta.getChecksum();
                    Checksum actual = null;
                    try {
                        actual = calculateCNChecksum(cn, sysMeta);
                    } catch (NotFound e) {
                        valid = false;
                    } catch (NotAuthorized e) {
                        valid = false;
                    } catch (NotImplemented e) {
                        valid = false;
                    } catch (InvalidToken e) {
                        // skip
                    } catch (ServiceFailure e) {
                        //skip
                    }
                    if (actual != null && valid) {
                        valid = ChecksumUtil.areChecksumsEqual(expected, actual);
                    }
                    if (!valid) {
                        invalidCN = nodeRef;
                        break;
                    }
                }
            }
        }
        if (valid) {
            updateReplicaVerified(sysMeta.getIdentifier(), replica);
        } else {
            log.error("CN replica is not valid for pid: " + sysMeta.getIdentifier() + " on CN: "
                    + invalidCN.getValue());
            //TODO: handle invalid CN relica
        }
    }

    private boolean isCNodeReplica(Replica replica) {
        return NodeType.CN.equals(hzNodes.get(replica.getReplicaMemberNode()).getType());
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
            cnMap.put(node.getIdentifier(), cn);
        }
        return cnMap.get(node.getIdentifier());
    }
}
