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
package org.dataone.service.cn.replication.auditor.v1.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.types.v1.Identifier;

/**
 * An abstract runnable that encapsulates processing callable auditing tasks.  
 * This processor uses an executor service to process tasks concurrently
 * in a thread pool.  Strategy is to batch tasks for the executor service, 
 * then handle the futures from the previous batch while the
 * current batch of tasks are executing.
 * 
 * @author sroseboo
 * 
 */
public abstract class AbstractReplicationAuditor implements Runnable {

    private static Logger log = Logger.getLogger(AbstractReplicationAuditor.class.getName());

    protected ReplicationDao replicationDao = DaoFactory.getReplicationDao();
    private ExecutorService executorService;

    public AbstractReplicationAuditor() {
        this.executorService = Executors.newFixedThreadPool(getTaskPoolSize());
    }

    protected abstract String getLockName();

    protected abstract Date calculateAuditDate();

    protected abstract List<Identifier> getPidsToAudit(Date auditDate, int pageNumber, int pageSize)
            throws DataAccessException;

    protected abstract Callable<String> newAuditTask(List<Identifier> pids, Date auditDate);

    protected abstract int getMaxPages();

    protected abstract int getTaskPoolSize();

    protected abstract int getPageSize();

    protected abstract int getPidsPerTaskSize();

    protected abstract boolean shouldRunAudit();

    protected abstract long getFutureExecutionWaitTimeSeconds();

    @Override
    public void run() {
        auditReplication();
    }

    public void auditReplication() {
        if (shouldRunAudit()) {
            Lock auditLock = getProcessingLock();
            try {
                if (tryLock(auditLock)) {
                    Date auditDate = calculateAuditDate();
                    List<Identifier> pidsToAudit = null;
                    for (int i = 1; i < getMaxPages(); i++) {
                        try {
                            pidsToAudit = getPidsToAudit(auditDate, i, getPageSize());
                        } catch (DataAccessException dae) {
                            log.error(
                                    "Unable to retrieve replicas by date using replication dao for audit date: "
                                            + auditDate.toString() + ".", dae);
                        }
                        if (pidsToAudit.size() == 0) {
                            break;
                        }
                        auditPids(pidsToAudit, auditDate);
                    }
                }
            } finally {
                releaseLock(auditLock);
            }
        }
    }

    protected boolean tryLock(Lock auditLock) {
        if (auditLock == null) {
            return false;
        }
        return auditLock.tryLock();
    }

    protected void releaseLock(Lock auditLock) {
        if (auditLock != null) {
            auditLock.unlock();
        }
    }

    protected Lock getProcessingLock() {
        return HazelcastClientFactory.getProcessingClient().getLock(getLockName());
    }

    private void auditPids(List<Identifier> pids, Date auditDate) {
        List<Identifier> pidBatch = new ArrayList<Identifier>();
        List<Callable<String>> auditTaskBatch = new ArrayList<Callable<String>>();
        List<Future> currentFutures = new ArrayList<Future>();
        List<Future> previousFutures = new ArrayList<Future>();

        for (Identifier pid : pids) {
            pidBatch.add(pid);
            if (pidBatch.size() >= getPidsPerTaskSize()) {
                auditTaskBatch.add(newAuditTask(pidBatch, auditDate));
                pidBatch.clear();
            }
            if (auditTaskBatch.size() >= getTaskPoolSize()) {
                submitTasks(auditTaskBatch, currentFutures, previousFutures);
            }
            if (!previousFutures.isEmpty()) {
                handleFutures(previousFutures);
            }
        }
        if (auditTaskBatch.size() > 0) {
            submitTasks(auditTaskBatch, currentFutures, previousFutures);
        }
        handleFutures(currentFutures);
    }

    private void submitTasks(List<Callable<String>> tasks, List<Future> currentFutures,
            List<Future> previousFutures) {

        previousFutures.clear();
        previousFutures.addAll(currentFutures);
        currentFutures.clear();
        for (Callable<String> auditTask : tasks) {
            submitTask(currentFutures, auditTask);
        }
        tasks.clear();
    }

    private void submitTask(List<Future> currentFutures, Callable<String> auditTask) {
        Future future = null;
        try {
            future = executorService.submit(auditTask);
        } catch (RejectedExecutionException rej) {
            log.error("Unable to submit tasks to executor service. ", rej);
            log.error("Sleeping for 10 seconds, trying again");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.error("sleep interrupted.", e);
            }
            try {
                future = executorService.submit(auditTask);
            } catch (RejectedExecutionException reEx) {
                log.error("Still unable to submit tasks to executor service, failing. ", reEx);
            }
        }
        if (future != null) {
            currentFutures.add(future);
        }
    }

    private void handleFutures(List<Future> taskFutures) {
        for (Future future : taskFutures) {
            handleFuture(future);
        }
    }

    private void handleFuture(Future future) {
        boolean isDone = false;
        String result = null;
        boolean timedOut = false;
        while (!isDone) {
            try {
                result = (String) future.get(getFutureExecutionWaitTimeSeconds(), TimeUnit.SECONDS);
                if (result != null) {
                    log.debug("Replica audit task completed with result: " + result);
                }
            } catch (InterruptedException e) {
                log.error("Replica audit task interrupted, cancelling.", e);
                future.cancel(true);
            } catch (CancellationException e) {
                log.error("Replica audit task cancelled.", e);
            } catch (ExecutionException e) {
                log.error("Replica audit task threw exception during execution. ", e);
            } catch (TimeoutException e) {
                if (timedOut == false) {
                    log.debug("Replica audit task timed out.  waiting another"
                            + getFutureExecutionWaitTimeSeconds() + " seconds.");
                    timedOut = true;
                } else {
                    log.error("Replica audit task timed out twice, cancelling.");
                    future.cancel(true);
                }
            }
            isDone = future.isDone();
        }
    }
}
