#include "Bson.hpp"

namespace fer
{

//////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////// VarBson ///////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

VarBson::VarBson(ModuleLoc loc, _bson_t *val, bool ownVal)
    : Var(loc, false, false), val(val), ownsVal(ownVal)
{}
VarBson::VarBson(ModuleLoc loc, const _bson_t *val)
    : Var(loc, false, false), val((bson_t *)val), ownsVal(false)
{}
VarBson::~VarBson() { clear(); }

Var *VarBson::onCopy(MemoryManager &mem, ModuleLoc loc)
{
    if(val) return makeVarWithRef<VarBson>(mem, loc, bson_copy(val), true);
    return makeVarWithRef<VarBson>(mem, loc, nullptr, false);
}
void VarBson::onSet(MemoryManager &mem, Var *from)
{
    if(val && ownsVal) clear();
    VarBson *f = as<VarBson>(from);
    val        = f->val;
    ownsVal    = false;
}

void VarBson::init()
{
    if(val && ownsVal) {
        bson_reinit(val);
    } else {
        val     = bson_new();
        ownsVal = true;
    }
}

void VarBson::clear()
{
    if(val && ownsVal) bson_destroy(val);
    val     = nullptr;
    ownsVal = false;
}

void VarBson::setView(const bson_t *viewVal)
{
    clear();
    val = (bson_t *)viewVal;
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////// VarBsonArrayBuilder /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

VarBsonArrayBuilder::VarBsonArrayBuilder(ModuleLoc loc, _bson_array_builder_t *val, bool ownVal)
    : Var(loc, false, false), val(val), ownsVal(ownVal)
{}
VarBsonArrayBuilder::~VarBsonArrayBuilder()
{
    if(ownsVal) bson_array_builder_destroy(val);
}

//////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////// Functions ////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

Var *newBson(VirtualMachine &vm, ModuleLoc loc, Span<Var *> args,
             const StringMap<AssnArgData> &assnArgs)
{
    return vm.makeVar<VarBson>(loc, bson_new(), true);
}

Var *newBsonView(VirtualMachine &vm, ModuleLoc loc, Span<Var *> args,
                 const StringMap<AssnArgData> &assnArgs)
{
    return vm.makeVar<VarBson>(loc, nullptr, false);
}

Var *newBsonArrayBuilder(VirtualMachine &vm, ModuleLoc loc, Span<Var *> args,
                         const StringMap<AssnArgData> &assnArgs)
{
    return vm.makeVar<VarBsonArrayBuilder>(loc, bson_array_builder_new(), true);
}

Var *bsonAppend(VirtualMachine &vm, ModuleLoc loc, Span<Var *> args,
                const StringMap<AssnArgData> &assnArgs)
{
    if(!args[1]->is<VarStr>()) {
        vm.fail(loc, "Expected the key to be a string, found: ", vm.getTypeName(args[1]));
        return nullptr;
    }
    bson_t *bs = as<VarBson>(args[0])->getVal();
    if(!bs) {
        vm.fail(loc, "bson object must be initialized before using here");
        return nullptr;
    }
    const String &key = as<VarStr>(args[1])->getVal();
    if(args[2]->is<VarNil>()) {
        bson_append_null(bs, key.c_str(), key.size());
    } else if(args[2]->is<VarBool>()) {
        bson_append_bool(bs, key.c_str(), key.size(), as<VarBool>(args[2])->getVal());
    } else if(args[2]->is<VarInt>()) {
        bson_append_int64(bs, key.c_str(), key.size(), as<VarInt>(args[2])->getVal());
    } else if(args[2]->is<VarFlt>()) {
        bson_append_double(bs, key.c_str(), key.size(), as<VarFlt>(args[2])->getVal());
    } else if(args[2]->is<VarStr>()) {
        const String &val = as<VarStr>(args[2])->getVal();
        bson_append_utf8(bs, key.c_str(), key.size(), val.c_str(), val.size());
    } else if(args[2]->is<VarBson>()) {
        const bson_t *val = as<VarBson>(args[2])->getVal();
        bson_append_document(bs, key.c_str(), key.size(), val);
    } else {
        Var *v = nullptr;
        Array<Var *, 1> tmp{args[2]};
        if(!vm.callVarAndExpect<VarBson>(loc, "bson", v, tmp, {})) return nullptr;
        bson_t *data = as<VarBson>(v)->getVal();
        bson_append_document(bs, key.c_str(), key.size(), data);
        vm.decVarRef(v);
    }
    return vm.getNil();
}

Var *bsonArrayBuilderAppend(VirtualMachine &vm, ModuleLoc loc, Span<Var *> args,
                            const StringMap<AssnArgData> &assnArgs)
{
    if(!args[1]->is<VarStr>()) {
        vm.fail(loc, "Expected the key to be a string, found: ", vm.getTypeName(args[1]));
        return nullptr;
    }
    bson_array_builder_t *arrBuilder = as<VarBsonArrayBuilder>(args[0])->getVal();
    if(args[1]->is<VarNil>()) {
        bson_array_builder_append_null(arrBuilder);
    } else if(args[1]->is<VarBool>()) {
        bson_array_builder_append_bool(arrBuilder, as<VarBool>(args[1])->getVal());
    } else if(args[1]->is<VarInt>()) {
        bson_array_builder_append_int64(arrBuilder, as<VarInt>(args[1])->getVal());
    } else if(args[1]->is<VarFlt>()) {
        bson_array_builder_append_double(arrBuilder, as<VarFlt>(args[1])->getVal());
    } else if(args[1]->is<VarStr>()) {
        const String &val = as<VarStr>(args[1])->getVal();
        bson_array_builder_append_utf8(arrBuilder, val.c_str(), val.size());
    } else if(args[1]->is<VarBson>()) {
        const bson_t *val = as<VarBson>(args[1])->getVal();
        bson_array_builder_append_document(arrBuilder, val);
    } else {
        Var *v = nullptr;
        Array<Var *, 1> tmp{args[1]};
        if(!vm.callVarAndExpect<VarBson>(loc, "bson", v, tmp, {})) return nullptr;
        bson_t *data = as<VarBson>(v)->getVal();
        bson_array_builder_append_document(arrBuilder, data);
        vm.decVarRef(v);
    }
    return vm.getNil();
}

Var *bsonArrayBuilderBuild(VirtualMachine &vm, ModuleLoc loc, Span<Var *> args,
                           const StringMap<AssnArgData> &assnArgs)
{
    VarBsonArrayBuilder *builder = as<VarBsonArrayBuilder>(args[0]);
    VarBson *res                 = vm.makeVar<VarBson>(loc, bson_new(), true);
    bson_array_builder_build(builder->getVal(), res->getVal());
    return res;
}

Var *bsonToStr(VirtualMachine &vm, ModuleLoc loc, Span<Var *> args,
               const StringMap<AssnArgData> &assnArgs)
{
    const bson_t *bs = as<VarBson>(args[0])->getVal();
    if(!bs) {
        vm.fail(loc, "bson object must be initialized before using here");
        return nullptr;
    }
    size_t len = 0;
    char *data = bson_as_canonical_extended_json(bs, &len);
    Var *res   = vm.makeVar<VarStr>(loc, data, len);
    bson_free(data);
    return res;
}

Var *bsonToBytebuffer(VirtualMachine &vm, ModuleLoc loc, Span<Var *> args,
                      const StringMap<AssnArgData> &assnArgs)
{
    const bson_t *bs = as<VarBson>(args[0])->getVal();
    if(!bs) {
        vm.fail(loc, "bson object must be initialized before using here");
        return nullptr;
    }
    const char *data = (const char *)bson_get_data(bs);
    return vm.makeVar<VarBytebuffer>(loc, bs->len, bs->len, data);
}

INIT_MODULE(Bson)
{
    vm.registerType<VarBson>(loc, "Bson");
    vm.registerType<VarBsonArrayBuilder>(loc, "BsonArrayBuilder");

    VarModule *mod = vm.getCurrModule();

    mod->addNativeFn(vm, "new", newBson);
    mod->addNativeFn(vm, "newView", newBsonView);
    mod->addNativeFn(vm, "newArrayBuilder", newBsonArrayBuilder);

    vm.addNativeTypeFn<VarBson>(loc, "append", bsonAppend, 2);
    vm.addNativeTypeFn<VarBson>(loc, "str", bsonToStr, 0);
    vm.addNativeTypeFn<VarBson>(loc, "bytes", bsonToBytebuffer, 0);

    vm.addNativeTypeFn<VarBsonArrayBuilder>(loc, "append", bsonArrayBuilderAppend, 1);
    vm.addNativeTypeFn<VarBsonArrayBuilder>(loc, "build", bsonArrayBuilderBuild, 0);
    return true;
}

} // namespace fer