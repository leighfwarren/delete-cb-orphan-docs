This tool will check for **Orphan documents in Couchbase** and remove them.

**Requires** a design/view to be created and published on couchbase (if the view is in development, use the flag -devView).

**The view must emit all the Hangers**.

VIEW CODE: 

```
function(doc, meta) {
 if (meta.id.indexOf('Hanger::') == 0) {
   emit(meta.id, null);
 }
}
```

BUILD:
```
mvn clean compile assembly:single
```
USAGE:
```
java -jar target/delete-cb-orphan-docs.jar -cbAddress localhost -cbBucket cmbucket -cbBucketPwd cmpasswd -design hangers -view hangers -dryRun
```
where:

-**cbAddress** is one Couchbase node address;

-**cbBucket** is the bucket name;

-**cbBucketPwd** is the bucket password;

-**design** is the design name in which the view has been created;

-**view** is the name of the view that emits all hangers;

-**devView** (Optional flag) if the view is still in development

-**dryRun** (Optional flag) is an option to run in dry mode (no real deletions will occur)

-**batchSize** (optional) is an option to limit the deletions to a specific number

-**limit** (optional) limit the result size of the view

-**skip** (optional) skip the first n records from the view

-**tidyUp** (optional) remove un-necessary data (WorkspaceBean, DraftContent, Activities)

-**fixData** (optional) Convert legacy NoSQL to new OneCMS Content types

-**numThreads** (optional) Number of threads to use for processing, default is 8

-**startKey** (optional) Key to start from in the view
