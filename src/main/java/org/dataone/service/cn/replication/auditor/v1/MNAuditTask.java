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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.v1.MNode;
import org.dataone.client.v1.itk.D1Client;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;

/**
 * Unused, ready to be removed.
 * 
 * 
 * A single audit task to be queued and executed by the Replication Service. The
 * audit task is generated from the result of a query on objects with replicas
 * that haven't been verified in 2 or more months.
 * 
 * @author dexternc
 * 
 */
public class MNAuditTask implements Serializable, Callable<String> {

    /* Get a Log instance */
    public static Log log = LogFactory.getLog(MNAuditTask.class);

    /* The identifier of this task */
    private String taskid;

    /*
     * The identifier of the system metadata map event that precipitated this
     * task
     */
    private String eventid;

    /* The target Node object */
    private Node auditTargetNode;

    /* The subject of the originating node, extracted from the Node object */
    private String auditTargetNodeSubject;

    /*
     * List of identifiers whose objects need to be checksummed on the target
     * Node
     */
    private ArrayList<Identifier> auditIDs;

    private Identifier last_pid;

    /**
     * Constructor - create an empty audit task instance
     */
    public MNAuditTask() {
    }

    /**
     * Constructor - create a audit task instance
     * 
     * @param taskid
     *            task ID for this task
     * @param auditTargetNode
     *            target Node to check
     * @param auditIDs
     *            list of IDs to check
     */
    public MNAuditTask(String taskid, Node auditTargetNode, ArrayList<Identifier> auditIDs) {
        this.taskid = taskid;
        this.auditTargetNode = auditTargetNode;
        this.auditTargetNodeSubject = auditTargetNode.getSubject(0).getValue();
        this.auditIDs = auditIDs;
    }

    /**
     * Get the task identifier for this task
     * 
     * @return the taskid
     */
    public String getTaskid() {
        return taskid;
    }

    /**
     * Set the task identifier for this task
     * 
     * @param taskid
     *            the taskid to set
     */
    public void setTaskid(String taskid) {
        this.taskid = taskid;
    }

    /**
     * Get the event identifier
     * 
     * @return the eventid
     */
    public String getEventid() {
        return eventid;
    }

    /**
     * Set the event identifier
     * 
     * @param eventid
     *            the eventid to set
     */
    public void setEventid(String eventid) {
        this.eventid = eventid;
    }

    /**
     * Get the target node
     * 
     * @return the targetNode
     */
    public Node getAuditTargetNode() {
        return auditTargetNode;
    }

    /**
     * Set the target node
     * 
     * @param targetNode
     *            the targetNode to set
     */
    public void setAuditTargetNode(Node auditTargetNode) {
        this.auditTargetNode = auditTargetNode;
    }

    /**
     * For the given Audit task, return the Subject listed in the audit target
     * node. Usually used in authorizing an audit event.
     * 
     * @return subject - the subject listed in the audit target Node object as a
     *         string
     */
    public String getAuditTargetNodeSubject() {
        return this.auditTargetNodeSubject;
    }

    /**
     * Set the target node subject identifying the node
     * 
     * @param subject
     *            the targetNode subject
     */
    public void setAuditTargetNodeSubject(String subject) {
        this.auditTargetNodeSubject = subject;
    }

    /**
     * Implement the Callable interface, providing code that initiates auditing.
     * 
     * @return pid - the identifier of the replicated object upon success
     */
    public String call() throws IllegalStateException {

        MNode targetMN = null;

        // Get an target MNode reference to communicate with
        try {
            // set up the certificate location
            String clientCertificateLocation = Settings.getConfiguration().getString(
                    "D1Client.certificate.directory")
                    + File.separator
                    + Settings.getConfiguration().getString("D1Client.certificate.filename");
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
            log.debug("MNReplicationTask task id " + this.taskid + "is using an X509 certificate "
                    + "from " + clientCertificateLocation);
            log.debug("Getting the MNode reference for "
                    + auditTargetNode.getIdentifier().getValue());
            targetMN = D1Client.getMN(auditTargetNode.getIdentifier());

        } catch (ServiceFailure e) {
            log.debug("Failed to get the target MNode reference for "
                    + auditTargetNode.getIdentifier().getValue()
                    + " while executing MNAuditTask id " + this.taskid);
        }

        // Get the D1 Hazelcast configuration parameters
        String hzSystemMetadata = Settings.getConfiguration().getString(
                "dataone.hazelcast.systemMetadata");

        // get the system metadata for the pid
        HazelcastClient hzClient = HazelcastClientFactory.getStorageClient();

        IMap<String, SystemMetadata> sysMetaMap = hzClient.getMap(hzSystemMetadata);

        // Initiate the MN checksum verification
        try {

            // the system metadata for the object to be checked.
            SystemMetadata sysmeta;

            // for each pid in the list of pids to be checked on this node
            for (Identifier pid : auditIDs) {

                this.last_pid = pid;

                // lock the pid we are going to check
                sysMetaMap.lock(pid.getValue());

                // get the system metadata to modify ReplicationStatus and
                // Replica for
                // this identifier
                sysmeta = sysMetaMap.get(pid);

                // list of replicas to check
                ArrayList<Replica> replicaList = (ArrayList<Replica>) sysmeta.getReplicaList();

                // the index of the Replica in the List<Replica>
                int replicaIndex = -1;

                for (Replica replica : replicaList) {
                    if (replica.getReplicaMemberNode().getValue()
                            .equals(auditTargetNode.getIdentifier().getValue())) {
                        replicaIndex = replicaList.indexOf(replica);
                        break;
                    }
                }

                if (replicaIndex == -1) {
                    throw new InvalidRequest("1080", "Node is not reported to have " + "object: "
                            + pid);
                }

                // call for the checksum audit
                log.debug("Calling MNRead.getChecksum() at auditTargetNode id "
                        + targetMN.getNodeId());

                // session is null - certificate is used
                Session session = null;

                // get the target checksum
                Checksum targetChecksum = 
                        targetMN.getChecksum(session, pid, sysmeta.getChecksum().getAlgorithm());

                if (targetChecksum != sysmeta.getChecksum()) {
                    replicaList.get(replicaIndex).setReplicationStatus(
                            ReplicationStatus.INVALIDATED);
                    replicaList.get(replicaIndex).setReplicaVerified(
                            Calendar.getInstance().getTime());

                } else {
                    replicaList.get(replicaIndex).setReplicaVerified(
                            Calendar.getInstance().getTime());
                }

                sysmeta.setReplicaList(replicaList);

                sysMetaMap.unlock(pid.getValue());
            }

        } catch (NotImplemented e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } catch (ServiceFailure e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } catch (NotAuthorized e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } catch (InvalidRequest e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } catch (InvalidToken e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } catch (NotFound e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } finally {
            sysMetaMap.unlock(this.last_pid.getValue());
        }

        return auditTargetNode.getIdentifier().getValue();
    }

}
