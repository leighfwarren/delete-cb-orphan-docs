package com.atex;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;

import java.util.NoSuchElementException;

public class Utils {

  private static Bucket bucket;
  private static boolean dryRun = false;

  public static void init(Bucket bucket, boolean dryRun) {
    Utils.bucket = bucket;
    Utils.dryRun = dryRun;
  }

  public static JsonDocument getItem(String id) {
    JsonDocument response = null;
    try {
      response = bucket.get(id);
    } catch (NoSuchElementException e) {
      System.out.println("ERROR: No element with message: "
          + e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("ERROR: getItem exception: " + e);
    }
    return response;
  }

  public static void deleteItem(String id) {
    try {
      if (!dryRun) {
        System.out.println("REMOVING: " + id);
        bucket.remove(id, PersistTo.MASTER);
      } else {
        System.out.println("(DRY-RUN) REMOVING: " + id);
      }
    } catch (DocumentDoesNotExistException e) {
      System.out.println("ERROR: DocumentDoesNotExistException: " + id);
    } catch (Exception e) {
      System.out.println("ERROR: deleteItem exception: " + e);
    }
  }

  private static void removeAspects(JsonDocument doc) {
    if (doc.content().containsKey("aspectLocations")) {
      JsonObject obj = doc.content().getObject("aspectLocations");
      for (String s : obj.getNames()) {
        String aspectId = obj.getString(s);
        aspectId = aspectId.replace("onecms:", "Aspect:");
        aspectId = aspectId.replace(":", "::");
        deleteItem(aspectId);
      }

    }
  }

  static boolean removeHanger(String hangerId) {
    JsonDocument doc = getItem(hangerId);
    if (doc != null) {
      removeAspects(doc);
      deleteItem(hangerId);
      return true;
    }
    return false;
  }

  static boolean removeHangerInfo(String hangerInfoId) {
    JsonDocument doc = getItem(hangerInfoId);
    if (doc != null) {
      JsonArray versions = doc.content().getArray("versions");
      for (Object version : versions) {
        JsonObject obj = (JsonObject) version;
        String _version = obj.getString("version");
        _version = _version.replace("onecms:", "Hanger:");
        _version = _version.replace(":", "::");
        removeHanger(_version);
      }
      removeAspects(doc);
      deleteItem(hangerInfoId);
      return true;
    }
    return false;
  }

  static String getHangerInfoFromHangerId(String hangerId) {
    String hangerInfoId = hangerId.replace("Hanger::", "HangerInfo::");
    hangerInfoId = hangerInfoId.substring(0, hangerInfoId.lastIndexOf("::"));
    return hangerInfoId;
  }

}
