﻿using System;
using System.Collections.Generic;

namespace LiteDB.Engine
{
    public interface ILiteEngine : IDisposable
    {
        int Checkpoint();
#if !NO_CREATE_INDEX // rebuild requires create index
        long Rebuild(RebuildOptions options);
#endif

        bool BeginTrans();
        bool Commit();
        bool Rollback();

        IBsonDataReader Query(string collection, Query query);

        int Insert(string collection, IEnumerable<BsonDocument> docs, BsonAutoId autoId);
        int Update(string collection, IEnumerable<BsonDocument> docs);
        int UpdateMany(string collection, BsonExpression transform, BsonExpression predicate);
        int Upsert(string collection, IEnumerable<BsonDocument> docs, BsonAutoId autoId);
        int Delete(string collection, IEnumerable<BsonValue> ids);
        int DeleteMany(string collection, BsonExpression predicate);

        bool DropCollection(string name);
#if !NO_RENAME_COLLECTION
        bool RenameCollection(string name, string newName);
#endif

#if !NO_CREATE_INDEX
        bool EnsureIndex(string collection, string name, BsonExpression expression, bool unique);
        bool DropIndex(string collection, string name);
#endif

        BsonValue Pragma(string name);
        bool Pragma(string name, BsonValue value);
    }
}