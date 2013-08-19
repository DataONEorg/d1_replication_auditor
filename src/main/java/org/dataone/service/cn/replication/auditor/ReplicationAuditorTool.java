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
package org.dataone.service.cn.replication.auditor;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dataone.service.cn.replication.auditor.v1.controller.ManualCoordinatingNodeReplicaAuditor;
import org.dataone.service.cn.replication.auditor.v1.controller.ManualMemberNodeReplicaAuditor;

public class ReplicationAuditorTool {

    private static Logger log = Logger.getLogger(ReplicationAuditorTool.class);

    public ReplicationAuditorTool() {
    }

    public static void main(String[] args) {

        PropertyConfigurator.configure("/etc/dataone/process/log4j.properties");

        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        Date dateParameter = null;
        String dateString = null;
        int options = 0;
        boolean cnAudit = false;
        boolean mnAudit = false;

        for (String arg : args) {
            if (StringUtils.startsWith(arg, "-d")) {
                dateString = StringUtils.substringAfter(arg, "-d");
                dateString = StringUtils.trim(dateString);
                try {
                    dateParameter = dateFormat.parse(dateString);
                } catch (ParseException e) {
                    System.out.println("Unable to parse provided date string: " + dateString);
                }
            } else if (StringUtils.startsWith(arg, "-cn")) {
                cnAudit = true;
                options++;
            } else if (StringUtils.startsWith(arg, "-mn")) {
                mnAudit = true;
                options++;
            }
        }

        if (dateParameter == null || options == 0) {
            showHelp();
            return;
        }
        System.out.println("Replication Auditing Starting....with options:");
        System.out.println("Audit Date:        " + dateFormat.format(dateParameter));
        System.out.println("Audit CN replicas: " + cnAudit);
        System.out.println("Audit MN replicas: " + mnAudit);
        System.out.println(" ");
        ReplicationAuditorTool auditor = new ReplicationAuditorTool();
        auditor.auditReplicas(dateParameter, cnAudit, mnAudit);
    }

    private void auditReplicas(Date auditDate, boolean auditCnReplicas, boolean auditMnReplicas) {
        if (auditDate == null) {
            return;
        }

        if (auditMnReplicas) {
            System.out.println("Starting Member Node replica auditing......");
            ManualMemberNodeReplicaAuditor mnReplicaAuditor = new ManualMemberNodeReplicaAuditor(
                    auditDate);
            mnReplicaAuditor.auditReplication();
            System.out.println("Member Node replica auditing complete.....");
        }

        if (auditCnReplicas) {
            System.out.println("Starting Coordinating Node replica auditing....");
            ManualCoordinatingNodeReplicaAuditor cnReplicaAuditor = new ManualCoordinatingNodeReplicaAuditor(
                    auditDate);
            cnReplicaAuditor.auditReplication();
            System.out.println("Coordinating Node replica auditing complete....");
        }
    }

    private static void showHelp() {
        System.out.println(" ");
        System.out.println("DataONE replica audit tool help:");
        System.out.println(" ");
        System.out
                .println("  Please note: Replica auditing requires use of the hazelcast 'processing'");
        System.out
                .println("    cluster - as such the processing daemon should be running when this");
        System.out.println("    is used.  It is ok to leave scheduled replica audit running.");
        System.out.println(" ");
        System.out
                .println("-d     REQUIRED. Replica auditing date.  Used to select replicas to audit.");
        System.out
                .println("       Replicas with a verified date before auditing date will be audited.");
        System.out.println("       Date format: mm/dd/yyyy.");
        System.out.println(" ");
        System.out
                .println("-mn    Tells the audit tool to audit Member Node replicas.  Includes original ");
        System.out.println("       auth member node copy auditing.");
        System.out.println(" ");
        System.out.println("-cn     Tells the audit tool to audit Coordinating Node replicas.");
        System.out.println(" ");
        System.out
                .println("Either or both 'cn' and/or 'mn' option must be specified.  If neither options ");
        System.out.println("are not specified, auditing will not run (you will see this message).");
        System.out.println(" ");
    }
}
