package com.atex;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.view.ViewQuery;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import rx.Observable;

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

    AtomicInteger processed = new AtomicInteger();
    AtomicInteger removed = new AtomicInteger();

    ViewQuery viewQuery;
    if (devView) {
      viewQuery = ViewQuery.from(design, view).development();
    } else {
      viewQuery = ViewQuery.from(design, view);
    }
    AsyncBucket asyncBucket = bucket.async();
    asyncBucket.query(viewQuery)
        .flatMap(results -> {
          System.out.println("==============================================================");
          System.out.println("========= Number of Hangers in the view: " + results.totalRows() + " ===========");
          System.out.println("==============================================================");
          return results.rows();
        })
        //.onBackpressureBuffer()
        //.skip(27)
        //.limit(1)
        .takeWhile((c) -> {
          //System.out.println("takeWhile Processed: " + removed.get() + " - BatchSize: " + batchSize);
          return batchSize <= 0 || removed.get() <= batchSize; // TODO: Investigate this, removed is not up to date while streaming rows
        })
        .flatMap(row -> {
          String hangerId = row.id();
          return Utils.getItemEx(Utils.getHangerInfoFromHangerId(hangerId)).singleOrDefault(null).flatMap(doc -> {
            if (doc == null) {
              removed.getAndIncrement();
              return Utils.removeHangerEx(hangerId);
              //.flatMap(result -> Observable.just(doc));  // result is a Boolean, uncomment this if you want to return JsonDocument
            }
            return Observable.just(false); // Return false, instead of empty otherwise subscribe is not called and processed is not incremented
          });
        })
        .onErrorResumeNext(throwable -> Observable.empty())
        .toBlocking()
        .subscribe(result -> {
          processed.addAndGet(1);
          if (processed.get() % 10000 == 0) {
            System.out.println("=== HANGERS PROCESSED: " + processed);
          }
        });

    System.out.println("==============================================================");
    System.out.println("====== Number of Orphan Hangers deleted: " + removed + " ========");
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
