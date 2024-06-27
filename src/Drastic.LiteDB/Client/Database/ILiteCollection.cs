﻿using System;
using System.Collections.Generic;
#if !NO_LINQ_EXPRESSION
using System.Linq.Expressions;
#endif

namespace LiteDB
{
    public interface ILiteCollection<T>
    {
        /// <summary>
        /// Get collection name
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Get collection auto id type
        /// </summary>
        BsonAutoId AutoId { get; }

#if !NO_ENTITY_MAPPER
        /// <summary>
        /// Getting entity mapper from current collection. Returns null if collection are BsonDocument type
        /// </summary>
        EntityMapper EntityMapper { get; }
#endif

#if !NO_INCLUDE_QUERY
#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Run an include action in each document returned by Find(), FindById(), FindOne() and All() methods to load DbRef documents
        /// Returns a new Collection with this action included
        /// </summary>
        ILiteCollection<T> Include<K>(Expression<Func<T, K>> keySelector);
#endif

        /// <summary>
        /// Run an include action in each document returned by Find(), FindById(), FindOne() and All() methods to load DbRef documents
        /// Returns a new Collection with this action included
        /// </summary>
        ILiteCollection<T> Include(BsonExpression keySelector);
#endif

        /// <summary>
        /// Insert or Update a document in this collection.
        /// </summary>
        bool Upsert(T entity);

        /// <summary>
        /// Insert or Update all documents
        /// </summary>
        int Upsert(IEnumerable<T> entities);

        /// <summary>
        /// Insert or Update a document in this collection.
        /// </summary>
        bool Upsert(BsonValue id, T entity);

        /// <summary>
        /// Update a document in this collection. Returns false if not found document in collection
        /// </summary>
        bool Update(T entity);

        /// <summary>
        /// Update a document in this collection. Returns false if not found document in collection
        /// </summary>
        bool Update(BsonValue id, T entity);

        /// <summary>
        /// Update all documents
        /// </summary>
        int Update(IEnumerable<T> entities);

#if !NO_WHERE_QUERY
        /// <summary>
        /// Update many documents based on transform expression. This expression must return a new document that will be replaced over current document (according with predicate).
        /// Eg: col.UpdateMany("{ Name: UPPER($.Name), Age }", "_id > 0")
        /// </summary>
        int UpdateMany(BsonExpression transform, BsonExpression predicate);

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Update many document based on merge current document with extend expression. Use your class with initializers. 
        /// Eg: col.UpdateMany(x => new Customer { Name = x.Name.ToUpper(), Salary: 100 }, x => x.Name == "John")
        /// </summary>
        int UpdateMany(Expression<Func<T, T>> extend, Expression<Func<T, bool>> predicate);
#endif
#endif

        /// <summary>
        /// Insert a new entity to this collection. Document Id must be a new value in collection - Returns document Id
        /// </summary>
        BsonValue Insert(T entity);

        /// <summary>
        /// Insert a new document to this collection using passed id value.
        /// </summary>
        void Insert(BsonValue id, T entity);

        /// <summary>
        /// Insert an array of new documents to this collection. Document Id must be a new value in collection. Can be set buffer size to commit at each N documents
        /// </summary>
        int Insert(IEnumerable<T> entities);

        /// <summary>
        /// Implements bulk insert documents in a collection. Usefull when need lots of documents.
        /// </summary>
        int InsertBulk(IEnumerable<T> entities, int batchSize = 5000);

#if !NO_CREATE_INDEX
        /// <summary>
        /// Create a new permanent index in all documents inside this collections if index not exists already. Returns true if index was created or false if already exits
        /// </summary>
        /// <param name="name">Index name - unique name for this collection</param>
        /// <param name="expression">Create a custom expression function to be indexed</param>
        /// <param name="unique">If is a unique index</param>
        bool EnsureIndex(string name, BsonExpression expression, bool unique = false);

        /// <summary>
        /// Create a new permanent index in all documents inside this collections if index not exists already. Returns true if index was created or false if already exits
        /// </summary>
        /// <param name="expression">Document field/expression</param>
        /// <param name="unique">If is a unique index</param>
        bool EnsureIndex(BsonExpression expression, bool unique = false);
#endif

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Create a new permanent index in all documents inside this collections if index not exists already.
        /// </summary>
        /// <param name="keySelector">LinqExpression to be converted into BsonExpression to be indexed</param>
        /// <param name="unique">Create a unique keys index?</param>
        bool EnsureIndex<K>(Expression<Func<T, K>> keySelector, bool unique = false);

        /// <summary>
        /// Create a new permanent index in all documents inside this collections if index not exists already.
        /// </summary>
        /// <param name="name">Index name - unique name for this collection</param>
        /// <param name="keySelector">LinqExpression to be converted into BsonExpression to be indexed</param>
        /// <param name="unique">Create a unique keys index?</param>
        bool EnsureIndex<K>(string name, Expression<Func<T, K>> keySelector, bool unique = false);
#endif

#if !NO_CREATE_INDEX
        /// <summary>
        /// Drop index and release slot for another index
        /// </summary>
        bool DropIndex(string name);
#endif

        /// <summary>
        /// Return a new LiteQueryable to build more complex queries
        /// </summary>
        ILiteQueryable<T> Query();

#if !NO_WHERE_QUERY
        /// <summary>
        /// Find documents inside a collection using predicate expression.
        /// </summary>
        IEnumerable<T> Find(BsonExpression predicate, int skip = 0, int limit = int.MaxValue);

        /// <summary>
        /// Find documents inside a collection using query definition.
        /// </summary>
        IEnumerable<T> Find(Query query, int skip = 0, int limit = int.MaxValue);

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Find documents inside a collection using predicate expression.
        /// </summary>
        IEnumerable<T> Find(Expression<Func<T, bool>> predicate, int skip = 0, int limit = int.MaxValue);
#endif
#endif

#if !NO_WHERE_QUERY
        /// <summary>
        /// Find a document using Document Id. Returns null if not found.
        /// </summary>
        T FindById(BsonValue id);

        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        T FindOne(BsonExpression predicate);

#if !EXPRESSION_PARSER_ONLY_FOR_INDEX
        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        T FindOne(string predicate, BsonDocument parameters);

        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        T FindOne(BsonExpression predicate, params BsonValue[] args);
#endif

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        T FindOne(Expression<Func<T, bool>> predicate);
#endif
#endif

        /// <summary>
        /// Find the first document using defined query structure. Returns null if not found
        /// </summary>
        T FindOne(Query query);

        /// <summary>
        /// Returns all documents inside collection order by _id index.
        /// </summary>
        IEnumerable<T> FindAll();

        /// <summary>
        /// Delete a single document on collection based on _id index. Returns true if document was deleted
        /// </summary>
        bool Delete(BsonValue id);

        /// <summary>
        /// Delete all documents inside collection. Returns how many documents was deleted. Run inside current transaction
        /// </summary>
        int DeleteAll();

#if !NO_WHERE_QUERY
        /// <summary>
        /// Delete all documents based on predicate expression. Returns how many documents was deleted
        /// </summary>
        int DeleteMany(BsonExpression predicate);

#if !EXPRESSION_PARSER_ONLY_FOR_INDEX
        /// <summary>
        /// Delete all documents based on predicate expression. Returns how many documents was deleted
        /// </summary>
        int DeleteMany(string predicate, BsonDocument parameters);

        /// <summary>
        /// Delete all documents based on predicate expression. Returns how many documents was deleted
        /// </summary>
        int DeleteMany(string predicate, params BsonValue[] args);
#endif

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Delete all documents based on predicate expression. Returns how many documents was deleted
        /// </summary>
        int DeleteMany(Expression<Func<T, bool>> predicate);
#endif
#endif

        /// <summary>
        /// Get document count using property on collection.
        /// </summary>
        int Count();

#if !NO_WHERE_QUERY
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        int Count(BsonExpression predicate);

#if !EXPRESSION_PARSER_ONLY_FOR_INDEX
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        int Count(string predicate, BsonDocument parameters);

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        int Count(string predicate, params BsonValue[] args);
#endif

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        int Count(Expression<Func<T, bool>> predicate);
#endif
#endif

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        int Count(Query query);

        /// <summary>
        /// Get document count using property on collection.
        /// </summary>
        long LongCount();

#if !NO_WHERE_QUERY
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long LongCount(BsonExpression predicate);

#if !EXPRESSION_PARSER_ONLY_FOR_INDEX
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long LongCount(string predicate, BsonDocument parameters);

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long LongCount(string predicate, params BsonValue[] args);
#endif

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long LongCount(Expression<Func<T, bool>> predicate);
#endif
#endif

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long LongCount(Query query);

#if !NO_WHERE_QUERY
        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        bool Exists(BsonExpression predicate);

#if !EXPRESSION_PARSER_ONLY_FOR_INDEX
        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        bool Exists(string predicate, BsonDocument parameters);

        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        bool Exists(string predicate, params BsonValue[] args);
#endif

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        bool Exists(Expression<Func<T, bool>> predicate);
#endif
#endif

        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        bool Exists(Query query);

#if !NO_ORDERBY_OR_GROUPBY_QUERY
        /// <summary>
        /// Returns the min value from specified key value in collection
        /// </summary>
        BsonValue Min(BsonExpression keySelector);

        /// <summary>
        /// Returns the min value of _id index
        /// </summary>
        BsonValue Min();

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Returns the min value from specified key value in collection
        /// </summary>
        K Min<K>(Expression<Func<T, K>> keySelector);
#endif

        /// <summary>
        /// Returns the max value from specified key value in collection
        /// </summary>
        BsonValue Max(BsonExpression keySelector);

        /// <summary>
        /// Returns the max _id index key value
        /// </summary>
        BsonValue Max();

#if !NO_LINQ_EXPRESSION
        /// <summary>
        /// Returns the last/max field using a linq expression
        /// </summary>
        K Max<K>(Expression<Func<T, K>> keySelector);
#endif
#endif
    }
}