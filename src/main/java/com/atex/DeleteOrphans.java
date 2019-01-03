package com.atex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import rx.Observable;
import rx.functions.Func1;

public class DeleteOrphans {

  public static final String NOSQL_IMAGE_TYPE = "com.atex.nosql.image.ImageContentDataBean";
  public static final String ATEX_ONECMS_IMAGE = "atex.onecms.image";
  public static final String ATEX_ONECMS_ARTICLE = "atex.onecms.article";
  private static final String NOSQL_ARTICLE_TYPE = "com.atex.nosql.article.ArticleBean";
  private static final String ARTCLE_BEAN_EXTENDED = "com.atex.nosql.article.ArticleBeanExtended";
  // Input values
  private static String cbAddress;
  private static String cbBucket;
  private static String cbBucketPwd;
  private static String design;
  private static String view;
  private static boolean devView = false;
  private static boolean dryRun = false;
  private static int batchSize = -1;

  private static final String CONTENT_ID_PREFIX = "onecms:";
  private static final String DELETION_PREFIX = "deletion:";
  private static final String MUTATION_PREFIX = "mutation:";

  private static Logger log = Logger.getLogger("Cleanup");

  private static volatile Map<String, Long> totals = new TreeMap<>();

  private static Bucket bucket;
  private static String startKey;
  private static int numThreads = 10;

  private static volatile int processed = 0;
  private static volatile int converted = 0;
  private static volatile int removed = 0;

  private static Set<String> deletedKeys = Collections.synchronizedSet(new HashSet<>());
  private static Set<String> convertedKeys  = Collections.synchronizedSet(new HashSet<>());

  private static JsonDocument getItem(String id) {
    JsonDocument response = null;
    try {
      response = bucket.get(id);
    } catch (NoSuchElementException e) {
      log.warning("No element with message: "
          + e.getMessage());
      e.printStackTrace();
    }
    return response;
  }

  private static void deleteItem(String id, List<String> keys) {
    if (!alreadyDeleted(id)) {
      if (!dryRun) {
        log.info("REMOVING: " + id);
        keys.add(id);
      } else {
        log.info("(DRY-RUN) REMOVING: " + id);
      }
    }
  }

  private static boolean alreadyDeleted(String id) {
    synchronized (deletedKeys) {
      if (deletedKeys.contains(id)) return true;
      deletedKeys.add(id);
      return false;
    }
  }

  private static boolean alreadyConverted(String id) {
    synchronized (convertedKeys) {
      if (convertedKeys.contains(id)) return true;
      convertedKeys.add(id);
      return false;
    }
  }

  private static void removeAspects(JsonDocument doc, List<String> keys) {
    if (doc.content().containsKey("aspectLocations")) {
      JsonObject obj = doc.content().getObject("aspectLocations");
      for (String s : obj.getNames()) {
        String aspectId = obj.getString(s);
        aspectId = aspectId.replace("onecms:", "Aspect:");
        aspectId = aspectId.replace(":", "::");
        deleteItem(aspectId, keys);
      }

    }
  }

  private static boolean removeHanger(String hangerId) {

    List<String> keys = new ArrayList<>();

    JsonDocument doc = getItem(hangerId);
    if (doc != null) {
      removeAspects(doc, keys);
      deleteItem(hangerId, keys);
      sendDeletes(keys);
    }
    return true;
  }

  private static String getHangerInfoFromHangerId(String hangerId) {
    String hangerInfoId = hangerId.replace("Hanger::", "HangerInfo::");
    hangerInfoId = hangerInfoId.substring(0, hangerInfoId.lastIndexOf("::"));
    return hangerInfoId;
  }

  private static void execute() throws Exception {

    String filename = "delete-orphans-" + new Date().getTime() + ".log";
    FileHandler fileHandler = new FileHandler(filename);
    SimpleFormatter simple = new SimpleFormatter();
    fileHandler.setFormatter(simple);
    log.addHandler(fileHandler);
    log.setUseParentHandlers(false);

    log.info ("Started @ " + new Date());

    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
            .connectTimeout(TimeUnit.SECONDS.toMillis(60L)) 
            .kvTimeout(TimeUnit.SECONDS.toMillis(60L))
            .viewTimeout(TimeUnit.SECONDS.toMillis(1200L))
            .maxRequestLifetime(TimeUnit.SECONDS.toMillis(1200L))
            .build();

    Cluster cluster = CouchbaseCluster.create(env, cbAddress);
    bucket = cluster.openBucket(cbBucket, cbBucketPwd);



    ViewQuery query = null;
    if (devView) {
      query = ViewQuery.from(design, view).development();
    } else {
      query = ViewQuery.from(design, view);
    }
    if (startKey != null) {
      query = query.startKey(startKey);
    }
    ViewResult result = bucket.query(query);
    float total = result.totalRows();
    log.info("Number of Hangers in the view: " + result.totalRows());
    log.info("Number of Threads: " + numThreads);
    long timeStarted = System.currentTimeMillis();

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    BoundedExecutor bex = new BoundedExecutor(executor, numThreads);

    for (ViewRow row : result) {
      bex.submitTask(() -> processRow(total, timeStarted, row.id()));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    bucket.close();
    cluster.disconnect();

    log.info ("Finished @ " + new Date());

    log.info("==============================================================");
    log.info("====== Number of Orphan Hangers processed: " + removed + " =========");
    log.info("====== Number of Legacy Hangers converted: " + converted + " =========");
    log.info("==============================================================");

    for (String key : totals.keySet()) {
      log.info(key + ", " + totals.get(key));
    }



    cluster.disconnect();
  }

  private static boolean processRow(float total, long timeStarted, String hangerId) {
    List<JsonDocument> updates = new ArrayList<>();
    processed++;

    String hangerInfoId = getHangerInfoFromHangerId(hangerId);
    JsonDocument hangerInfo = getItem(hangerInfoId);
    if (hangerInfo == null) {
      if (removeHanger(hangerId)) {
        removed++;
      }
    } else {
      boolean needsIndexing = false;
      JsonDocument hanger = getItem(hangerId);
      String type = hanger.content().getString("type");
      accumlateTotals (type);
      if (type.equalsIgnoreCase(NOSQL_IMAGE_TYPE)) {
        needsIndexing = convertAspect(hangerId, hanger, NOSQL_IMAGE_TYPE, ATEX_ONECMS_IMAGE, "com.atex.onecms.app.dam.standard.aspects.OneImageBean", false, updates);
        converted++;
      } else if (type.equalsIgnoreCase(NOSQL_ARTICLE_TYPE)) {
        needsIndexing = convertAspect(hangerId, hanger, NOSQL_ARTICLE_TYPE, ATEX_ONECMS_ARTICLE, "com.atex.onecms.app.dam.standard.aspects.CustomArticleBean", true, updates);
        converted++;
      }
      if (needsIndexing) {
        sendKafkaMutation (hangerId, hangerInfo, updates);
      }
    }
    if (!updates.isEmpty()) sendUpdates(updates);

    if (batchSize > 0 && (removed + converted) >= batchSize) {
      return true;
    }

    if (processed % 10000 == 0) {
      float percentage = processed  * 100 / total;
      String out = String.format("%f", percentage);
      long now = System.currentTimeMillis();
      float duration = now - timeStarted;
      long endTime = (long) (now + ((total - processed) * (duration / processed)));

      log.info("=== HANGERS PROCESSED: " + processed + ", %age = " + out + ", ETA : "+ new Date(endTime));
    }
    return false;
  }

  private static void sendUpdates(List<JsonDocument> items) {
    //items.forEach(doc -> System.out.println(doc.id()));
    Observable
            .from(items)
            .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
              @Override
              public Observable<JsonDocument> call(final JsonDocument docToInsert) {
                return bucket.async().replace(docToInsert).onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {
                  @Override
                  public Observable<? extends JsonDocument> call(Throwable throwable) {
                    log.warning ("Error processing doc " + docToInsert.id() + " : " + throwable);
                    return Observable.error(throwable);
                  }
                });
              }
            })
            .onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {
              @Override
              public Observable<? extends JsonDocument> call(Throwable throwable) {
                log.warning ("Error processing batch " +  " : " + throwable);
                return Observable.error(throwable);
              }
            })
            .retryWhen(RetryBuilder
                    .anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
                    .max(10)
                    .build())
            .last()
            .toBlocking()
            .single();
  }

  private static void sendDeletes(List<String> keys) {
    Observable
            .from(keys)
            .flatMap(new Func1<String, Observable<JsonDocument>>() {
              @Override
              public Observable<JsonDocument> call(final String key) {
                return bucket.async().remove(key).onErrorResumeNext(
                        new Func1<Throwable, Observable<? extends JsonDocument>>() {
                          @Override
                          public Observable<? extends JsonDocument> call(Throwable throwable) {
                            log.warning ("Error removing key " + key + " : " + throwable);
                            return Observable.empty();
                          }
                        });
              }
            })
            .doOnError(throwable -> log.log (Level.WARNING, "Error processing batch : " + throwable))
            .retryWhen(RetryBuilder
                    .anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
                    .max(10)
                    .build())
            .last()
            .toBlocking()
            .single();

  }

  private static void sendKafkaMutation(String hangerId, JsonDocument hangerInfo, List<JsonDocument> updates) {
    String message = createMutationMessage (hangerInfo);

    log.info(message);

    if (!alreadyConverted(hangerInfo.id())) updates.add (hangerInfo);

  }


  private static String getAspectIdFromContentId(String aspectContentId) {
    return "Aspect::" + aspectContentId.replace("onecms:", "").replace(":", "::");
  }

  private static synchronized void accumlateTotals(String type) {
    long value = 0;
    if (totals.containsKey(type)) {
      value = totals.get(type).longValue();

    }
    value++;

    totals.put(type, value);

  }

  private static boolean convertAspect(String hangerId, JsonDocument hanger,
                                       String sourceType, String targetType,
                                       String targetBean, boolean convertWireArticles,
                                       List<JsonDocument> updates) {
    boolean needsIndexing = false;
    JsonObject aspects = hanger.content().getObject("aspectLocations");
    String aspectContentId = aspects.getString(sourceType);
    if (aspectContentId!=null && !alreadyConverted(aspectContentId)) {
      String aspectId = getAspectIdFromContentId(aspectContentId);

      JsonDocument aspect = getItem(aspectId);
      if (aspect != null && aspect.content().getString("name").equalsIgnoreCase(sourceType)) {
        aspect.content().put("name", targetType);
        JsonObject data = aspect.content().getObject("data");
        data.put("_type", targetBean);

        if (data.containsKey("creationDate")) {
          long creationDate = data.getLong("creationDate");
          data.removeKey("creationDate");

          data.put("creationdate", getDateObject(creationDate));
          accumlateTotals("Creation date converted");
        }

        if (data.containsKey("timeState")) {
          JsonObject tso = data.getObject("timeState");

          tso.put ("_type", "com.atex.onecms.app.dam.types.TimeState");
          accumlateTotals("Creation date converted");
        }

        // This is no longer in OneArticleBean
        data.removeKey("editorsPickHeadline");

        String name = (String) data.get("name");
        if (convertWireArticles && name != null && name.matches(".*PRESSUK_.*")) {
          data.put ("inputTemplate", "p.DamWireArticle");
          accumlateTotals("Converted to Wire Article");
          needsIndexing = true;
        }

        /* Article bean extended is not used anymore, so will migrate the data into the standard bean */
        String extendedContentId = aspects.getString(ARTCLE_BEAN_EXTENDED);
        if (extendedContentId != null) {
          String extendedAspectId = getAspectIdFromContentId(extendedContentId);

          JsonDocument extended = getItem(extendedAspectId);
          JsonObject extendedData = extended.content().getObject("data");

          for (String key : extendedData.getNames()) {
            if (!key.equalsIgnoreCase(("_type"))){
              data.put(key, extendedData.get(key));
            }
          }

          aspects.removeKey(ARTCLE_BEAN_EXTENDED);
          accumlateTotals("Extended article bean aspect converted and removed");
        }
        if (!dryRun) {
          log.info ("Converting Aspect " + aspectContentId + " to " + targetType);
          updates.add (aspect);
        } else {
          log.info ("TEST  "+ aspectContentId + " to " + targetType);
        }
      }
      // Need to remove and replace aspect just in case the versions have not changed.
      if (aspects.containsKey(sourceType)) {
        aspects.removeKey(sourceType);
        aspects.put(targetType, aspectContentId);
      }
    }
    hanger.content().put("type", targetType);

    if (!dryRun) {
      log.info ("Converting Hanger " + hangerId + " to " + targetType);
      updates.add (hanger);
    } else {
      log.info ("Test Hanger " + hangerId + " to " + targetType);
    }
    return needsIndexing;
  }



  private static JsonObject getDateObject(long creationDate) {
    Date d = new Date (creationDate);
    String dateJson = String.format("{\"_type\": \"java.util.Date\",\"time\": %d}", d.getTime());

    return JsonObject.fromJson(dateJson);
  }

  public static void main(String[] args) throws Exception {
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
    options.addOption("startKey", true, "Starting ID if re-starting the process after failure");
    options.addOption("numThreads", true, "Number of threads to use, default 10");


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
      if (cmdLine.hasOption("numThreads")) {
        numThreads = Integer.parseInt(cmdLine.getOptionValue("numThreads"));
      }

      if (cmdLine.hasOption("startKey")) {
        startKey = cmdLine.getOptionValue("startKey");
      }

    } catch (Exception e) {
      e.printStackTrace();
      formatter.printHelp("DeleteOrphans", options);
      System.exit(-99);
    }

    execute();
  }

  private static String createMutationMessage(final JsonDocument hangerInfo)
  {

    String cvid = getCvid(hangerInfo);
    if (cvid != null) {
      return MUTATION_PREFIX + cvid;
    }
    return null;
  }

  /**
   * Using "raw" json since we do not want dependencies on HangerInfo et. al.
   */
  private static String getCvid(JsonDocument doc)
  {
    JsonObject top = doc.content();
    if (top.containsKey("versions")) {
      JsonArray versions = top.getArray("versions");
      if (versions.size() > 0) {
        JsonObject latestVersion = (JsonObject) versions.get(versions.size() - 1);
        return latestVersion.getString("version");
      }
    }
    return null;
  }

    public static class BoundedExecutor {
        private final Executor exec;
        private final Semaphore semaphore;
        public BoundedExecutor(Executor exec, int bound) {
            this.exec = exec;
            this.semaphore = new Semaphore(bound);
        }
        public void submitTask(final Runnable command)
                throws InterruptedException {
            semaphore.acquire();
            try {
                exec.execute(new Runnable() {
                    public void run() {
                        try {
                            command.run();
                        } finally {
                            semaphore.release();
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                semaphore.release();
            }
        }
    }

}
