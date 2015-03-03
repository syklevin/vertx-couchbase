# Vert.x 3.0 couchbase service

## insert

``` java

JsonObject insert = new JsonObject()
    .put("doctype", "player")
    .put("id", "player:123812904821")
    .put("upsert", true)
    .put("content", new JsonObject()
        .put("name", "man god")
    );

cbService.insert(insert, ar -> {});

```

## findOne

``` java

JsonObject findOne = new JsonObject()
    .put("doctype", "player")
    .put("id", "player:123812904821"");

cbService.findOne(findOne, ar -> {});

```

## deleteOne

``` java

JsonObject deleteOne = new JsonObject()
    .put("doctype", "player")
    .put("id", "player:123812904821"");

cbService.deleteOne(findOne, ar -> {});

```

## update

using mongodb like syntax
actions: $set, $push, $pull, $addToSet

``` java

JsonObject update = new JsonObject()
    .put("doctype", "player")
    .put("upsert", true) /* not found insert new one */
    .put("update", new JsonObject())
        .put("$set", new JsonObject()
            .put("age", 18)
        )
    );

cbService.update(update, ar -> { });

```

## view query

``` java

JsonObject viewQuery = new JsonObject()
    .put("design", "dev_player")
    .put("view", "find_by_id")
    .put("limit", 10)
    .put("skip", 10)
    .put("group", true)
    .put("group_level", 2)
    .put("reduce", true)
    .put("descending", true)
    .put("key", "12389012839")
    .put("keys", new JsonArray().add(...))
    .put("endKey", ...)
    .put("startKeY", ...);

cbService.viewQuery(viewQuery, ar -> { });

```

## bulk

support action: insert, findOne, deleteOne, update

``` java

JsonArray actions = new JsonArray()
    .add(new JsonObject()
        .put("_actionName", "insert")
        .put("doctype", "player")
        .put("id", "2901831290839")
    );
JsonObject bulk = new JsonObject()
    .put("actions", actions);

cbService.bulk(bulk, ar ->  { });

```
