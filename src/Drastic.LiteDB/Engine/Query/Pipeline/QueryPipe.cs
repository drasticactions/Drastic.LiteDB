﻿using System;
using System.Collections.Generic;
using System.Linq;
using static LiteDB.Constants;

namespace LiteDB.Engine
{
    /// <summary>
    /// Basic query pipe workflow - support filter, includes and orderby
    /// </summary>
    internal class QueryPipe : BasePipe
    {
        public QueryPipe(TransactionService transaction, IDocumentLookup loader, SortDisk tempDisk, EnginePragmas pragmas, uint maxItemsCount)
            : base(transaction, loader, tempDisk, pragmas, maxItemsCount)
        {
        }

        /// <summary>
        /// Query Pipe order
        /// - LoadDocument
        /// - IncludeBefore
        /// - Filter
        /// - OrderBy
        /// - OffSet
        /// - Limit
        /// - IncludeAfter
        /// - Select
        /// </summary>
        public override IEnumerable<BsonDocument> Pipe(IEnumerable<IndexNode> nodes, QueryPlan query)
        {
            // starts pipe loading document
            var source = this.LoadDocument(nodes);

#if !NO_WHERE_QUERY
            // do includes in result before filter
            foreach (var path in query.IncludeBefore)
            {
                source = this.Include(source, path);
            }

            // filter results according expressions
            foreach (var expr in query.Filters)
            {
                source = this.Filter(source, expr);
            }
#endif

#if !NO_ORDERBY_OR_GROUPBY_QUERY
            if (query.OrderBy != null)
            {
                // pipe: orderby with offset+limit
                source = this.OrderBy(source, query.OrderBy.Expression, query.OrderBy.Order, query.Offset, query.Limit);
            }
            else
#endif
            {
                // pipe: apply offset (no orderby)
                if (query.Offset > 0) source = source.Skip(query.Offset);

                // pipe: apply limit (no orderby)
                if (query.Limit < int.MaxValue) source = source.Take(query.Limit);
            }

#if !NO_WHERE_QUERY
            // do includes in result after filter
            foreach (var path in query.IncludeAfter)
            {
                source = this.Include(source, path);
            }
#endif

            // if is an aggregate query, run select transform over all resultset - will return a single value
            if (query.Select.All)
            {
                return this.SelectAll(source, query.Select.Expression);
            }
            // run select transform in each document and return a new document or value
            else
            {
                return this.Select(source, query.Select.Expression);
            }
        }

        /// <summary>
        /// Pipe: Transaform final result appling expressin transform. Can return document or simple values
        /// </summary>
        private IEnumerable<BsonDocument> Select(IEnumerable<BsonDocument> source, BsonExpression select)
        {
            var defaultName = select.DefaultFieldName();

            foreach (var doc in source)
            {
#if INVARIANT_CULTURE
                var value = select.ExecuteScalar(doc);
#else
                var value = select.ExecuteScalar(doc, _pragmas.Collation);
#endif

                if (value.IsDocument)
                {
                    yield return value.AsDocument;
                }
                else
                {
                    yield return new BsonDocument { [defaultName] = value };
                }
            }
        }

        /// <summary>
        /// Pipe: Run select expression over all recordset
        /// </summary>
        private IEnumerable<BsonDocument> SelectAll(IEnumerable<BsonDocument> source, BsonExpression select)
        {
            var cached = new DocumentCacheEnumerable(source, _lookup);

            var defaultName = select.DefaultFieldName();
#if INVARIANT_CULTURE
            var result = select.Execute(cached);
#else
            var result = select.Execute(cached, _pragmas.Collation);
#endif

            foreach (var value in result)
            {
                if (value.IsDocument)
                {
                    yield return value.AsDocument;
                }
                else
                {
                    yield return new BsonDocument { [defaultName] = value };
                }
            }
        }
    }
}