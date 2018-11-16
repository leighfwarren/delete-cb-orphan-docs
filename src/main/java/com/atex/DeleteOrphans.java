package com.atex;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.concurrent.atomic.AtomicInteger;

public class DeleteOrphans {

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
    AtomicInteger removed = new AtomicInteger();
    ViewResult result;
    if (devView) {
      result = bucket.query(ViewQuery.from(design, view).development());
    } else {
      result = bucket.query(ViewQuery.from(design, view));
    }
    System.out.println("==============================================================");
    System.out.println("========= Number of Hangers in the view: " + result.totalRows() + " ===========");
    System.out.println("==============================================================");
    for (ViewRow row : result) {
      processed++;
      String hangerId = row.id();
      String hangerInfoId = Utils.getHangerInfoFromHangerId(hangerId);

      // Try async
      Utils.getItemEx(hangerInfoId).map(item -> {
        if (item == null) {
          Utils.removeH(hangerInfoId).map( doc -> {
            if (doc != null) {
              removed.getAndIncrement();
              if (batchSize > 0 && removed.get() >= batchSize) {
                // How to exit here!!!!
              }
            }
            return doc;
          });
        }
        return item;
      });


      /*if (Utils.getItem(hangerInfoId) == null) {
        if (Utils.removeHanger(hangerId)) {
          removed++;
          if (batchSize > 0 && removed >= batchSize) {
            break;
          }
        }
      }
      if (processed % 100000 == 0) {
        System.out.println("=== HANGERS PROCESSED: " + processed);
      }*/
    }
    System.out.println("==============================================================");
    System.out.println("====== Number of Orphan Hangers processed: " + removed + " =========");
    System.out.println("==============================================================");

    cluster.disconnect();
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
