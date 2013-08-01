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
package org.dataone.service.cn.replication.auditor.log;

public class AuditLogClientFactory {

    private static AuditLogClient solrClient = new AuditLogClientSolrImpl();

    private AuditLogClientFactory() {
    }

    public static AuditLogClient getAuditLogClient() {
        return solrClient;
    }

    public static void main(String[] args) throws InterruptedException {
        AuditLogClient alc = AuditLogClientFactory.getAuditLogClient();
        System.out.println(alc.queryLog("*:*", null, 0));
        AuditLogEntry rale = new AuditLogEntry("test-pid-4", "urn:node:the616",
                AuditEvent.REPLICA_NOT_FOUND, "This doc is missing!");
        alc.logAuditEvent(rale);
        //        Thread.sleep(2000);
        //        System.out.println(alc.queryLog("*:*", null, 0));
        //        alc.removeReplicaAuditEvent(new AuditLogEntry("test-pid-1", null,
        //                AuditEvent.REPLICA_BAD_CHECKSUM, null, null));
        Thread.sleep(2000);
        System.out.println(alc.queryLog(new AuditLogEntry("test-pid-4", null, null, null, null),
                null, null));
    }
}
