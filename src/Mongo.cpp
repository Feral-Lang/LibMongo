
#include "Mongo.hpp"

#include "Bson.hpp"

namespace fer
{

//////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////// VarMongoClient ///////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

VarMongoClient::VarMongoClient(ModuleLoc loc, const char *uriStr) : Var(loc, false, false)
{
    val = mongoc_client_new(uriStr);
}
VarMongoClient::~VarMongoClient() { mongoc_client_destroy(val); }

//////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////// VarMongoCollection /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

VarMongoCollection::VarMongoCollection(ModuleLoc loc, VarMongoClient *client, StringRef dbName,
                                       StringRef collectionName)
    : Var(loc, false, false), parent(client), dbName(dbName), collName(collectionName)
{}
VarMongoCollection::~VarMongoCollection() {}

void VarMongoCollection::onCreate(MemoryManager &mem)
{
    Var::incVarRef(parent);
    val = mongoc_client_get_collection(parent->getVal(), dbName.c_str(), collName.c_str());
}
void VarMongoCollection::onDestroy(MemoryManager &mem)
{
    mongoc_collection_destroy(val);
    Var::decVarRef(mem, parent);
}

//////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////// VarMongoCursor ///////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

VarMongoCursor::VarMongoCursor(ModuleLoc loc, VarMongoCollection *collection,
                               mongoc_cursor_t *cursor)
    : Var(loc, false, false), parent(collection), val(cursor)
{}
VarMongoCursor::~VarMongoCursor() {}

void VarMongoCursor::onCreate(MemoryManager &mem) { Var::incVarRef(parent); }
void VarMongoCursor::onDestroy(MemoryManager &mem)
{
    mongoc_cursor_destroy(val);
    Var::decVarRef(mem, parent);
}

//////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////// VarMongoCursorIter /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

VarMongoCursorIter::VarMongoCursorIter(ModuleLoc loc, mongoc_cursor_t *cursor)
    : Var(loc, false, false), val(cursor), iter(nullptr)
{}
VarMongoCursorIter::~VarMongoCursorIter() {}

bool VarMongoCursorIter::next(const bson_t *&res, bson_error_t *bErr)
{
    res = nullptr;
    if(!mongoc_cursor_next(val, &iter)) { return mongoc_cursor_error(val, bErr); }
    res = iter;
    return true;
}

//////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////// Functions ////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

FERAL_FUNC(newClient, 1, false,
           "  fn(uri) -> MongoClient\n"
           "Creates and returns a connection to the MongoDB `url`.")
{
    EXPECT(VarStr, args[1], "connection uri");
    return vm.makeVar<VarMongoClient>(loc, as<VarStr>(args[1])->getVal().c_str());
}

FERAL_FUNC(clientGetDatabaseNamesNative, 0, false, "")
{
    VarMongoClient *client = as<VarMongoClient>(args[0]);
    const bson_t *opts     = args[1]->is<VarBson>() ? as<VarBson>(args[1])->getVal() : nullptr;
    bson_error_t err;
    char **dbs = mongoc_client_get_database_names_with_opts(client->getVal(), opts, &err);
    if(!dbs) {
        vm.fail(loc, "failed to get database names, mongo error: ", err.message);
        return nullptr;
    }
    VarVec *res = vm.makeVar<VarVec>(loc, 0, false);
    for(size_t i = 0; dbs[i]; ++i) res->push(vm.makeVarWithRef<VarStr>(loc, dbs[i]));
    bson_strfreev(dbs);
    return res;
}

FERAL_FUNC(clientGetCollection, 2, false,
           "  var.fn(dbName, collectionName) -> MongoCollection\n"
           "Returns the MongoCollection for the given `dbName` and `collectionName` from within "
           "MongoClient `var`.")
{
    EXPECT(VarStr, args[1], "database name");
    EXPECT(VarStr, args[2], "collection name");
    VarMongoClient *client = as<VarMongoClient>(args[0]);
    StringRef db           = as<VarStr>(args[1])->getVal();
    StringRef coll         = as<VarStr>(args[2])->getVal();
    return vm.makeVar<VarMongoCollection>(loc, client, db, coll);
}

FERAL_FUNC(collectionInsertOneNative, 2, false, "")
{
    EXPECT(VarBson, args[1], "map representing the data");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    const bson_t *doc        = as<VarBson>(args[1])->getVal();
    const bson_t *opts       = args[2]->is<VarBson>() ? as<VarBson>(args[2])->getVal() : nullptr;
    bson_error_t berr;
    if(!mongoc_collection_insert_one(coll->getVal(), doc, opts, nullptr, &berr)) {
        size_t len = 0;
        char *bs   = bson_as_canonical_extended_json(doc, &len);
        vm.fail(loc, "failed to insert document `", bs,
                "` in collection: ", mongoc_collection_get_name(coll->getVal()),
                "; mongo err: ", berr.message);
        bson_free(bs);
        return nullptr;
    }
    return vm.getNil();
}

FERAL_FUNC(collectionInsertManyNative, 2, false, "")
{
    EXPECT(VarVec, args[1], "maps of data to insert");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    VarVec *docsVec          = as<VarVec>(args[1]);
    size_t docsCount         = docsVec->size();
    const bson_t **docs =
        (const bson_t **)vm.getMemoryManager().allocRaw(docsVec->size(), alignof(_bson_t));
    for(size_t i = 0; i < docsCount; ++i) { docs[i] = as<VarBson>(docsVec->at(i))->getVal(); }
    const bson_t *opts = args[2]->is<VarBson>() ? as<VarBson>(args[2])->getVal() : nullptr;
    bson_error_t berr;
    if(!mongoc_collection_insert_many(coll->getVal(), docs, docsCount, opts, nullptr, &berr)) {
        vm.fail(loc, "failed to insert document list in collection: ",
                mongoc_collection_get_name(coll->getVal()), "; mongo err: ", berr.message);
        vm.getMemoryManager().freeRaw(docs);
        return nullptr;
    }
    vm.getMemoryManager().freeRaw(docs);
    return vm.getNil();
}

FERAL_FUNC(collectionReplaceNative, 3, false, "")
{
    EXPECT(VarBson, args[1], "map representing the filter");
    EXPECT(VarBson, args[2], "map representing the new data");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    const bson_t *filter     = as<VarBson>(args[1])->getVal();
    const bson_t *data       = as<VarBson>(args[2])->getVal();
    const bson_t *opts       = args[3]->is<VarBson>() ? as<VarBson>(args[3])->getVal() : nullptr;
    bson_error_t berr;
    if(!mongoc_collection_replace_one(coll->getVal(), filter, data, opts, nullptr, &berr)) {
        size_t lenFilter = 0, lenData = 0;
        char *bsFilter = bson_as_canonical_extended_json(filter, &lenFilter);
        char *bsData   = bson_as_canonical_extended_json(data, &lenData);
        vm.fail(loc, "failed to replace document `", bsData, "` in collection `",
                mongoc_collection_get_name(coll->getVal()), "` using `", bsFilter,
                "` as the filter; mongo err: ", berr.message);
        bson_free(bsData);
        bson_free(bsFilter);
        return nullptr;
    }
    return vm.getNil();
}

FERAL_FUNC(collectionUpdateOneNative, 3, false, "")
{
    EXPECT(VarBson, args[1], "map representing the filter");
    EXPECT(VarBson, args[2], "map representing the new data");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    const bson_t *filter     = as<VarBson>(args[1])->getVal();
    const bson_t *data       = as<VarBson>(args[2])->getVal();
    const bson_t *opts       = args[3]->is<VarBson>() ? as<VarBson>(args[3])->getVal() : nullptr;
    bson_error_t berr;
    if(!mongoc_collection_update_one(coll->getVal(), filter, data, opts, nullptr, &berr)) {
        size_t lenFilter = 0, lenData = 0;
        char *bsFilter = bson_as_canonical_extended_json(filter, &lenFilter);
        char *bsData   = bson_as_canonical_extended_json(data, &lenData);
        vm.fail(loc, "failed to update document `", bsData, "` in collection `",
                mongoc_collection_get_name(coll->getVal()), "` using `", bsFilter,
                "` as the filter; mongo err: ", berr.message);
        bson_free(bsData);
        bson_free(bsFilter);
        return nullptr;
    }
    return vm.getNil();
}

FERAL_FUNC(collectionUpdateManyNative, 3, false, "")
{
    EXPECT(VarBson, args[1], "map representing the filter");
    EXPECT(VarBson, args[2], "map representing the new data");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    const bson_t *filter     = as<VarBson>(args[1])->getVal();
    const bson_t *data       = as<VarBson>(args[2])->getVal();
    const bson_t *opts       = args[3]->is<VarBson>() ? as<VarBson>(args[3])->getVal() : nullptr;
    bson_error_t berr;
    if(!mongoc_collection_update_many(coll->getVal(), filter, data, opts, nullptr, &berr)) {
        size_t lenFilter = 0, lenData = 0;
        char *bsFilter = bson_as_canonical_extended_json(filter, &lenFilter);
        char *bsData   = bson_as_canonical_extended_json(data, &lenData);
        vm.fail(loc, "failed to update document `", bsData, "` in collection `",
                mongoc_collection_get_name(coll->getVal()), "` using `", bsFilter,
                "` as the filter; mongo err: ", berr.message);
        bson_free(bsData);
        bson_free(bsFilter);
        return nullptr;
    }
    return vm.getNil();
}

FERAL_FUNC(collectionDeleteOneNative, 2, false, "")
{
    EXPECT(VarBson, args[1], "map representing the filter");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    const bson_t *filter     = as<VarBson>(args[1])->getVal();
    const bson_t *opts       = args[2]->is<VarBson>() ? as<VarBson>(args[2])->getVal() : nullptr;
    bson_error_t berr;
    if(!mongoc_collection_delete_one(coll->getVal(), filter, opts, nullptr, &berr)) {
        size_t len = 0;
        char *bs   = bson_as_canonical_extended_json(filter, &len);
        vm.fail(loc, "failed to delete document using filter `", bs, "` in collection `",
                mongoc_collection_get_name(coll->getVal()), "; mongo err: ", berr.message);
        bson_free(bs);
        return nullptr;
    }
    return vm.getNil();
}

FERAL_FUNC(collectionDeleteManyNative, 2, false, "")
{
    EXPECT(VarBson, args[1], "map representing the filter");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    const bson_t *filter     = as<VarBson>(args[1])->getVal();
    const bson_t *opts       = args[2]->is<VarBson>() ? as<VarBson>(args[2])->getVal() : nullptr;
    bson_error_t berr;
    if(!mongoc_collection_delete_many(coll->getVal(), filter, opts, nullptr, &berr)) {
        size_t len = 0;
        char *bs   = bson_as_canonical_extended_json(filter, &len);
        vm.fail(loc, "failed to delete document using filter `", bs, "` in collection `",
                mongoc_collection_get_name(coll->getVal()), "; mongo err: ", berr.message);
        bson_free(bs);
        return nullptr;
    }
    return vm.getNil();
}

FERAL_FUNC(collectionFindNative, 2, false, "")
{
    EXPECT(VarBson, args[1], "map representing the filter");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    const bson_t *filter     = as<VarBson>(args[1])->getVal();
    const bson_t *opts       = args[2]->is<VarBson>() ? as<VarBson>(args[2])->getVal() : nullptr;
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(coll->getVal(), filter, opts, nullptr);
    if(!cursor) {
        vm.fail(loc, "failed to get a cursor from collection find");
        return nullptr;
    }
    return vm.makeVar<VarMongoCursor>(loc, coll, cursor);
}

FERAL_FUNC(collectionGetName, 0, false,
           "  var.fn() -> Str\n"
           "Returns the name of the MongoCollection `var`.")
{
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    return vm.makeVar<VarStr>(loc, coll->getCollectionName());
}

FERAL_FUNC(collectionGetDatabaseName, 0, false,
           "  var.fn() -> Str\n"
           "Returns the name of the database which holds the MongoCollection `var`.")
{
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    return vm.makeVar<VarStr>(loc, coll->getDbName());
}

FERAL_FUNC(collectionLenNative, 2, false, "")
{
    EXPECT(VarBson, args[1], "map representing the filter");
    VarMongoCollection *coll = as<VarMongoCollection>(args[0]);
    const bson_t *filter     = as<VarBson>(args[1])->getVal();
    const bson_t *opts       = args[2]->is<VarBson>() ? as<VarBson>(args[2])->getVal() : nullptr;
    bson_error_t berr;
    int64_t count =
        mongoc_collection_count_documents(coll->getVal(), filter, opts, nullptr, nullptr, &berr);
    if(count < 0) {
        vm.fail(loc, "failed to count documents in collection: ",
                mongoc_collection_get_name(coll->getVal()), "; mongo err: ", berr.message);
        return nullptr;
    }
    return vm.makeVar<VarInt>(loc, count);
}

FERAL_FUNC(cursorEach, 0, false,
           "  var.fn() -> MongoCursorIter\n"
           "Creates and returns and iterator for MongoCursor `var`.")
{
    VarMongoCursor *curr = as<VarMongoCursor>(args[0]);
    return vm.makeVar<VarMongoCursorIter>(loc, curr->getVal());
}

FERAL_FUNC(cursorIterNext, 0, false,
           "  var.fn() -> Bson | Nil\n"
           "Returns the next Bson in the MongoCursorIter `var`, or `nil` if nothing remains.")
{
    VarMongoCursorIter *currIter = as<VarMongoCursorIter>(args[0]);
    const bson_t *bs             = nullptr;
    bson_error_t bErr;
    if(!currIter->next(bs, &bErr)) return vm.getNil();
    if(!bs) {
        vm.fail(loc, "failed to get next element using cursor, Mongo error: ", bErr.message);
        return nullptr;
    }
    Var *res = vm.makeVar<VarBson>(loc, bs);
    res->setLoadAsRef();
    return res;
}

INIT_MODULE(Mongo)
{
    mongoc_init();

    VarModule *mod = vm.getCurrModule();

    vm.registerType<VarMongoClient>(loc, "MongoClient",
                                    "Represents a connection to a MongoDB instance.");
    vm.registerType<VarMongoCollection>(loc, "MongoCollection",
                                        "Represents a collection in a Mongo database.");
    vm.registerType<VarMongoCursor>(
        loc, "MongoCursor",
        "Represents a return value(s) holder using which an iterator can be created.");
    vm.registerType<VarMongoCursorIter>(
        loc, "MongoCursorIter",
        "Represents an iterator that iterates through items in MongoCursor.");

    mod->addNativeFn(vm, "newClient", newClient);

    vm.addNativeTypeFn<VarMongoClient>(loc, "getDatabaseNamesNative", clientGetDatabaseNamesNative);
    vm.addNativeTypeFn<VarMongoClient>(loc, "getCollection", clientGetCollection);

    vm.addNativeTypeFn<VarMongoCollection>(loc, "insertOneNative", collectionInsertOneNative);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "insertManyNative", collectionInsertManyNative);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "replaceNative", collectionReplaceNative);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "updateOneNative", collectionUpdateOneNative);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "updateManyNative", collectionUpdateManyNative);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "deleteOneNative", collectionDeleteOneNative);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "deleteManyNative", collectionDeleteManyNative);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "findNative", collectionFindNative);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "getName", collectionGetName);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "getDatabaseName", collectionGetDatabaseName);
    vm.addNativeTypeFn<VarMongoCollection>(loc, "lenNative", collectionLenNative);

    vm.addNativeTypeFn<VarMongoCursor>(loc, "each", cursorEach);
    vm.addNativeTypeFn<VarMongoCursorIter>(loc, "next", cursorIterNext);

    return true;
}

DEINIT_MODULE(Mongo) { mongoc_cleanup(); }

} // namespace fer