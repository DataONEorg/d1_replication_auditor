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

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;
import org.dataone.service.cn.replication.v1.ReplicationTaskQueue;
import org.dataone.service.types.v1.NodeReference;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

/**
 * 
 * Periodically runs to clear out the 'queued' replication tasks.
 * Transitions all queued replicas to requested.  Purely a process to aid
 * replication processing from deadlocking the task queue.
 * 
 * @author sroseboo
 *
 */
public class QueuedReplicationAuditor implements Runnable {

    private static Log log = LogFactory.getLog(QueuedReplicationAuditor.class);

    private ReplicationTaskQueue replicationTaskQueue = new ReplicationTaskQueue();
    private ReplicationDao replicationDao = DaoFactory.getReplicationDao();
    private static final String QUEUED_REPLICATION_LOCK_NAME = "queuedReplicationAuditingLock";
    private static HazelcastInstance hzMember = HazelcastInstanceFactory.getProcessingInstance();

    public QueuedReplicationAuditor() {
    }

    @Override
    public void run() {
        if (ComponentActivationUtility.replicationIsActive()) {
            boolean isLocked = false;
            ILock lock = hzMember.getLock(QUEUED_REPLICATION_LOCK_NAME);
            try {
                isLocked = lock.tryLock();
                if (isLocked) {
                    log.debug("Queued Request Auditor running.");
                    runQueuedTasks();
                    log.debug("Queued Replication Auditor finished.");
                }
            } catch (Exception e) {
                log.error("Error processing queued replicas:", e);
            } finally {
                if (isLocked) {
                    lock.unlock();
                }
            }
        }
    }

    private void runQueuedTasks() {
        Collection<NodeReference> nodes = replicationTaskQueue.getMemberNodesInQueue();
        for (NodeReference nodeRef : nodes) {
            int sizeOfQueue = replicationTaskQueue.getCountOfTasksForNode(nodeRef.getValue());
            log.debug("Queued tasks for member node: " + nodeRef.getValue() + " has: "
                    + sizeOfQueue + " tasks in queue.");
            if (sizeOfQueue > 0) {
                int sizeOfRequested = getRequestedCount(nodeRef);
                log.debug("Queued Auditor report for mn: " + nodeRef.getValue() + " has: "
                        + sizeOfRequested + " requested replicas and: " + sizeOfQueue
                        + " requested replicas.");
                replicationTaskQueue.processAllTasksForMN(nodeRef.getValue());
            }
        }
    }

    private int getRequestedCount(NodeReference nodeRef) {
        int sizeOfRequested = -1;
        try {
            sizeOfRequested = replicationDao.getRequestedReplicationCount(nodeRef);
        } catch (DataAccessException e) {
            log.error("Unable to get oustanding rplication count for mn: " + nodeRef.getValue(), e);
        }
        return sizeOfRequested;
    }
}
