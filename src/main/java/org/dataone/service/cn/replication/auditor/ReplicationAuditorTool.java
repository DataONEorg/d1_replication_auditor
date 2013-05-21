package org.dataone.service.cn.replication.auditor;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.dataone.service.cn.replication.auditor.v1.ManualCoordinatingNodeReplicaAuditor;
import org.dataone.service.cn.replication.auditor.v1.ManualMemberNodeReplicaAuditor;

public class ReplicationAuditorTool {

    public ReplicationAuditorTool() {
    }

    public static void main(String[] args) {
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

        if (dateParameter == null) {
            showHelp();
            return;
        }

        if (options == 0) {
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
        System.out.println("DataONE replica audit tool help:");
        System.out.println(" ");
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
    }
}
