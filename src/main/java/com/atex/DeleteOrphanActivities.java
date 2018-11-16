package com.atex;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class DeleteOrphanActivities {

  // Input values
  private static String cbAddress;
  private static String cbBucket;
  private static String cbBucketPwd;
  private static String design;
  private static String view;
  private static boolean devView = false;
  private static boolean dryRun = false;
  private static int batchSize = -1;

  private static void execute() {
    Cluster cluster = CouchbaseCluster.create(cbAddress);
    Bucket bucket = cluster.openBucket(cbBucket, cbBucketPwd);

    Utils.init(bucket, dryRun);

    int processed = 0;
    int removed = 0;
    ViewResult result;
    if (devView) {
      result = bucket.query(ViewQuery.from(design, view).development());
    } else {
      result = bucket.query(ViewQuery.from(design, view));
    }
    System.out.println("==============================================================");
    System.out.println("========= Number of HangersAlias in the view: " + result.totalRows() + " ===========");
    System.out.println("==============================================================");
    for (ViewRow row : result) {
      processed++;
      String hangerAliasId = row.id();
      RawJsonDocument str = row.document(RawJsonDocument.class);
      if (str == null) {
        System.out.println("NULL RawJsonDocument for: " + hangerAliasId);
        continue;
      }
      String aliasContent = str.content();
      if (processHangerAlias(hangerAliasId, aliasContent)) {
        removed++;
        if (batchSize > 0 && removed >= batchSize) {
          break;
        }
      }
      if (processed % 100000 == 0) {
        System.out.println("=== HANGER ALIAS PROCESSED: " + processed);
      }
    }
    System.out.println("==============================================================");
    System.out.println("====== Number of Orphan Hangers processed: " + removed + " =========");
    System.out.println("==============================================================");

    cluster.disconnect();
  }

  private static boolean processHangerAlias(String aliasId, String aliasContent) {
    String contentHangerInfoId = aliasId.replace("HangerAlias::externalId::onecms:", "HangerInfo::");
    String activityServiceHangerInfoId = aliasContent.replace("\"", "");
    activityServiceHangerInfoId = activityServiceHangerInfoId.replace("onecms:", "HangerInfo::");
    if (Utils.getItem(contentHangerInfoId) == null) {
      System.out.println("The content " + contentHangerInfoId + " has been removed");
      System.out.println("We need to clear recursively " + activityServiceHangerInfoId);
      if (Utils.removeHangerInfo(activityServiceHangerInfoId)) {
        Utils.deleteItem(aliasId);
        return true;
      }
    }
    return false;
  }

  public static void main(String[] args) {
    Options options = new Options();
    HelpFormatter formatter = new HelpFormatter();
    options.addOption("cbAddress", true, "One Couchbase node address");
    options.addOption("cbBucket", true, "The bucket name");
    options.addOption("cbBucketPwd", true, "The bucket password");
    options.addOption("design", true, "The hangers design name");
    options.addOption("view", true, "The hangers design view");
    options.addOption("devView", false, "the view is in development (Optional)");
    options.addOption("dryRun", false, "To just output the docs to be deleted (Optional)");
    options.addOption("batchSize", true, "Limit to a number of hanger deletions (Optional)");


    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine cmdLine = parser.parse(options, args);
      if (cmdLine.hasOption("cbAddress")) {
        cbAddress = cmdLine.getOptionValue("cbAddress");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("cbBucket")) {
        cbBucket = cmdLine.getOptionValue("cbBucket");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("cbBucketPwd")) {
        cbBucketPwd = cmdLine.getOptionValue("cbBucketPwd");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("design")) {
        design = cmdLine.getOptionValue("design");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("view")) {
        view = cmdLine.getOptionValue("view");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("devView")) {
        devView = true;
      }
      if (cmdLine.hasOption("dryRun")) {
        dryRun = true;
      }
      if (cmdLine.hasOption("batchSize")) {
        batchSize = Integer.parseInt(cmdLine.getOptionValue("batchSize"));
      }

    } catch (Exception e) {
      e.printStackTrace();
      formatter.printHelp("DeleteOrphans", options);
      System.exit(-99);
    }

    execute();
  }
}
