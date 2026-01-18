#pragma once

#include <mongoc/mongoc.h>
#include <VM/Interpreter.hpp>

namespace fer
{

//////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////// VarMongoClient ///////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

class VarMongoClient : public Var
{
    mongoc_client_t *val;

public:
    VarMongoClient(ModuleLoc loc, const char *uriStr);
    ~VarMongoClient();

    inline mongoc_client_t *&getVal() { return val; }
};

//////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////// VarMongoCollection /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

class VarMongoCollection : public Var
{
    VarMongoClient *parent;
    mongoc_collection_t *val;
    String dbName;
    String collName;

    void onCreate(MemoryManager &mem) override;
    void onDestroy(MemoryManager &mem) override;

public:
    // Must store client to make sure it does not get deleted before the collection does.
    VarMongoCollection(ModuleLoc loc, VarMongoClient *client, StringRef dbName,
                       StringRef collectionName);
    ~VarMongoCollection();

    inline mongoc_collection_t *&getVal() { return val; }
    inline StringRef getDbName() { return dbName; }
    inline StringRef getCollectionName() { return collName; }
};

//////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////// VarMongoCursor ///////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

class VarMongoCursor : public Var
{
    VarMongoCollection *parent;
    mongoc_cursor_t *val;

    void onCreate(MemoryManager &mem) override;
    void onDestroy(MemoryManager &mem) override;

public:
    // Must store collection to make sure it does not get deleted before the cursor does.
    VarMongoCursor(ModuleLoc loc, VarMongoCollection *collection, mongoc_cursor_t *cursor);
    ~VarMongoCursor();

    inline mongoc_cursor_t *&getVal() { return val; }
};

//////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////// VarMongoCursorIter /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

// Essentially same as VarMongoCursor, except this doesn't own the cursor.
class VarMongoCursorIter : public Var
{
    mongoc_cursor_t *val;
    const bson_t *iter;

public:
    VarMongoCursorIter(ModuleLoc loc, mongoc_cursor_t *cursor);
    ~VarMongoCursorIter();

    bool next(const bson_t *&res, bson_error_t *bErr);
};

} // namespace fer