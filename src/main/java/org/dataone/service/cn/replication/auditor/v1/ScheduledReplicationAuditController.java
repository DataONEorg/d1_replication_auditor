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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.cn.replication.auditor.v1.controller.CoordinatingNodeReplicationAuditor;
import org.dataone.service.cn.replication.auditor.v1.controller.InvalidMemberNodeReplicationAuditor;
import org.dataone.service.cn.replication.auditor.v1.controller.MemberNodeReplicationAuditor;

/**
 * Controller responsible for scheduling(starting) and stopping replication auditors.
 * 
 * @author sroseboo
 *
 */
public class ScheduledReplicationAuditController {

    private static Log logger = LogFactory.getLog(ScheduledReplicationAuditController.class
            .getName());

    private static ScheduledExecutorService staleRequestedReplicaAuditScheduler;
    private static ScheduledExecutorService staleQueuedReplicaAuditScheduler;
    private static ScheduledExecutorService mnReplicaAuditScheduler;
    private static ScheduledExecutorService invalidMnReplicaAuditScheduler;
    private static ScheduledExecutorService cnReplicaAuditScheduler;

    public ScheduledReplicationAuditController() {

    }

    public void startup() {
        logger.info("starting scheduled replication auditing...");
        startStaleRequestedAuditing();
        startStaleQueuedAuditing();
        startMnReplicaAuditing();
        startCnReplicaAuditing();
        logger.info("scheduled replication auditing started.");
    }

    public void shutdown() {
        logger.info("stopping scheduled replication auditing....");
        if (staleRequestedReplicaAuditScheduler != null) {
            staleRequestedReplicaAuditScheduler.shutdown();
        }
        if (staleQueuedReplicaAuditScheduler != null) {
            staleQueuedReplicaAuditScheduler.shutdown();
        }
        if (mnReplicaAuditScheduler != null) {
            mnReplicaAuditScheduler.shutdown();
        }
        if (invalidMnReplicaAuditScheduler != null) {
            invalidMnReplicaAuditScheduler.shutdown();
        }
        if (cnReplicaAuditScheduler != null) {
            cnReplicaAuditScheduler.shutdown();
        }
        logger.info("scheduled replication auditing stopped.");
    }

    private void startStaleQueuedAuditing() {
        if (staleQueuedReplicaAuditScheduler == null
                || staleQueuedReplicaAuditScheduler.isShutdown()) {
            staleQueuedReplicaAuditScheduler = Executors.newSingleThreadScheduledExecutor();
            staleQueuedReplicaAuditScheduler.scheduleAtFixedRate(new QueuedReplicationAuditor(),
                    0L, 1L, TimeUnit.HOURS);
        }
    }

    private void startStaleRequestedAuditing() {
        if (staleRequestedReplicaAuditScheduler == null
                || staleRequestedReplicaAuditScheduler.isShutdown()) {
            staleRequestedReplicaAuditScheduler = Executors.newSingleThreadScheduledExecutor();
            staleRequestedReplicaAuditScheduler.scheduleAtFixedRate(
                    new StaleReplicationRequestAuditor(), 0L, 1L, TimeUnit.HOURS);
        }
    }

    private void startMnReplicaAuditing() {
        if (mnReplicaAuditScheduler == null || mnReplicaAuditScheduler.isShutdown()) {
            mnReplicaAuditScheduler = Executors.newSingleThreadScheduledExecutor();
            mnReplicaAuditScheduler.scheduleAtFixedRate(new MemberNodeReplicationAuditor(), 0L, 1L,
                    TimeUnit.HOURS);
        }
        if (invalidMnReplicaAuditScheduler == null || invalidMnReplicaAuditScheduler.isShutdown()) {
            invalidMnReplicaAuditScheduler = Executors.newSingleThreadScheduledExecutor();
            invalidMnReplicaAuditScheduler.scheduleAtFixedRate(
                    new InvalidMemberNodeReplicationAuditor(), 0L, 1L, TimeUnit.HOURS);
        }
    }

    private void startCnReplicaAuditing() {
        if (cnReplicaAuditScheduler == null || cnReplicaAuditScheduler.isShutdown()) {
            cnReplicaAuditScheduler = Executors.newSingleThreadScheduledExecutor();
            cnReplicaAuditScheduler.scheduleAtFixedRate(new CoordinatingNodeReplicationAuditor(),
                    0L, 1L, TimeUnit.HOURS);
        }
    }
}
