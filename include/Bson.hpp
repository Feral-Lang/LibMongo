#pragma once

#include <bson/bson.h>
#include <VM/Interpreter.hpp>

namespace fer
{

//////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////// VarBson ///////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

class VarBson : public Var
{
    bson_t *val;
    bool ownsVal;

    Var *onCopy(MemoryManager &mem, ModuleLoc loc) override;
    void onSet(MemoryManager &mem, Var *from) override;

public:
    // Must use _bson_t here otherwise clang gives warning:
    // Passing 4-byte aligned argument to 8-byte aligned parameter 2
    // See this for explanation:
    // https://stackoverflow.com/questions/78685283/using-stdunique-ptr-with-aligned-type-results-in-compiler-warning
    VarBson(ModuleLoc loc, _bson_t *val, bool ownVal);
    VarBson(ModuleLoc loc, const _bson_t *val);
    ~VarBson();

    void init();
    void clear();

    void setView(const bson_t *viewVal);

    inline bson_t *&getVal() { return val; }
    inline bool isInitialized() { return val; }
};

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////// VarBsonArrayBuilder /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

class VarBsonArrayBuilder : public Var
{
    bson_array_builder_t *val;
    bool ownsVal;

public:
    // Must use _bson_t here otherwise clang gives warning:
    // Passing 4-byte aligned argument to 8-byte aligned parameter 2
    // See this for explanation:
    // https://stackoverflow.com/questions/78685283/using-stdunique-ptr-with-aligned-type-results-in-compiler-warning
    VarBsonArrayBuilder(ModuleLoc loc, _bson_array_builder_t *val, bool ownVal);
    ~VarBsonArrayBuilder();

    inline bson_array_builder_t *&getVal() { return val; }
};

} // namespace fer