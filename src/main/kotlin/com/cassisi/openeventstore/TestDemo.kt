package com.cassisi.openeventstore

import RecordLayerDemoProto
import RecordLayerDemoProto.Flower
import com.apple.foundationdb.record.RecordMetaData
import com.apple.foundationdb.record.RecordMetaDataBuilder
import com.apple.foundationdb.record.metadata.Index
import com.apple.foundationdb.record.metadata.Key
import com.apple.foundationdb.record.provider.foundationdb.*
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory
import com.apple.foundationdb.record.query.RecordQuery
import com.apple.foundationdb.record.query.expressions.Query
import com.apple.foundationdb.tuple.Tuple
import com.google.protobuf.Message
import java.util.UUID
import java.util.function.Function


fun main() {
    val db = FDBDatabaseFactory.instance().database


    // Define the keyspace for our application
    val keySpace =
        KeySpace(KeySpaceDirectory("record-layer-demo", KeySpaceDirectory.KeyType.STRING, "record-layer-demo"))

// Get the path where our record store will be rooted
    val path = keySpace.path("record-layer-demo")

    val metaDataBuilder: RecordMetaDataBuilder = RecordMetaData.newBuilder()
        .setRecords(RecordLayerDemoProto.getDescriptor())

    val id = UUID.randomUUID().toString()

    metaDataBuilder.getRecordType("Order")
        //.setPrimaryKey(Key.Expressions.field("order_id"));
        .setPrimaryKey(Key.Expressions.value(id))

    metaDataBuilder.addIndex("Order", Index("priceIndex", Key.Expressions.field("price")))

    val recordMetaData = metaDataBuilder.build()

    val recordStoreProvider: (FDBRecordContext) -> FDBRecordStore = { context ->
        FDBRecordStore.newBuilder()
            .setMetaDataProvider(recordMetaData)
            .setContext(context)
            .setKeySpacePath(path)
            .createOrOpen()
    }

    db.run { context ->
        val recordStore = recordStoreProvider(context)

        recordStore.saveRecord(
            RecordLayerDemoProto.Order.newBuilder()
                .setOrderId(1)
                .setPrice(123)
                .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.RED))
                .build()
        );
        recordStore.saveRecord(
            RecordLayerDemoProto.Order.newBuilder()
                .setOrderId(23)
                .setPrice(34)
                .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.PINK))
                .build()
        );
        recordStore.saveRecord(
            RecordLayerDemoProto.Order.newBuilder()
                .setOrderId(3)
                .setPrice(55)
                .setFlower(buildFlower(FlowerType.TULIP, RecordLayerDemoProto.Color.YELLOW))
                .build()
        );
        recordStore.saveRecord(
            RecordLayerDemoProto.Order.newBuilder()
                .setOrderId(100)
                .setPrice(9)
                .setFlower(buildFlower(FlowerType.LILY, RecordLayerDemoProto.Color.RED))
                .build()
        );
    }

    val storedRecord =
        db.run { context ->
            recordStoreProvider(context).loadRecord(Tuple.from(id))
        }

    checkNotNull(storedRecord)


    // a record that doesn't exist is null
    val shouldNotExist: FDBStoredRecord<Message?>? =
        db.run { context ->
            recordStoreProvider(context).loadRecord(Tuple.from(99999))
        }
    assert(shouldNotExist == null)

    val order = RecordLayerDemoProto.Order.newBuilder()
        .mergeFrom(storedRecord.getRecord())
        .build()
    println(order)

    val query = RecordQuery.newBuilder()
        .setRecordType("Order")
        .setFilter(
            Query.and(
                Query.field("price").lessThan(50),
                Query.field("flower").matches(Query.field("type").equalsValue(FlowerType.ROSE.name))
            )
        )
        .build()

    println("------------")

    db.run { context ->
        val recordStore = recordStoreProvider(context)

        val cursor = recordStore.executeQuery(query)

        // let's return it as a list of records
        cursor.map { RecordLayerDemoProto.Order.newBuilder()
            .mergeFrom(it.record)
            .build()
        }.asList().join()
    }.forEach { println(it) }

    //OnlineIndexer

}

private enum class FlowerType {
    ROSE,
    TULIP,
    LILY,
}

private fun buildFlower(type: FlowerType, color: RecordLayerDemoProto.Color?): Flower {
    return Flower.newBuilder()
        .setType(type.name)
        .setColor(color)
        .build()
}