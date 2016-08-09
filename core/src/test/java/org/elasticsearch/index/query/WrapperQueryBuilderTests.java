/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class WrapperQueryBuilderTests extends AbstractQueryTestCase<WrapperQueryBuilder> {

    @Override
    protected boolean supportsBoostAndQueryName() {
        return false;
    }

    @Override
    protected WrapperQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder wrappedQuery = RandomQueryBuilder.createQuery(random());
        switch (randomInt(2)) {
            case 0:
                return new WrapperQueryBuilder(wrappedQuery.toString());
            case 1:
                return new WrapperQueryBuilder(BytesReference.toBytes(((ToXContentToBytes)wrappedQuery).buildAsBytes()));
            case 2:
                return new WrapperQueryBuilder(((ToXContentToBytes)wrappedQuery).buildAsBytes());
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected void doAssertLuceneQuery(WrapperQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        QueryBuilder innerQuery = queryBuilder.rewrite(createShardContext());
        Query expected = rewrite(innerQuery.toQuery(context));
        assertEquals(rewrite(query), expected);
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder((byte[]) null));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder(new byte[0]));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder((String) null));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder(""));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder((BytesReference) null));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder(new BytesArray(new byte[0])));
    }

    /**
     * Replace the generic test from superclass, wrapper query only expects
     * to find `query` field with nested query and should throw exception for
     * anything else.
     */
    @Override
    public void testUnknownField() throws IOException {
        String json = "{ \"" + WrapperQueryBuilder.NAME + "\" : {\"bogusField\" : \"someValue\"} }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertTrue(e.getMessage().contains("bogusField"));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"wrapper\" : {\n" +
                "    \"query\" : \"e30=\"\n" +
                "  }\n" +
                "}";


        WrapperQueryBuilder parsed = (WrapperQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        try {
            assertEquals(json, "{}", new String(parsed.source(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void testMustRewrite() throws IOException {
        TermQueryBuilder tqb = new TermQueryBuilder("foo", "bar");
        WrapperQueryBuilder qb = new WrapperQueryBuilder(tqb.toString());
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> qb.toQuery(createShardContext()));
        assertEquals("this query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = qb.rewrite(createShardContext());
        assertEquals(tqb, rewrite);
    }

    public void testRewriteWithInnerName() throws IOException {
        QueryBuilder builder = new WrapperQueryBuilder("{ \"match_all\" : {\"_name\" : \"foobar\"}}");
        QueryShardContext shardContext = createShardContext();
        assertEquals(new MatchAllQueryBuilder().queryName("foobar"), builder.rewrite(shardContext));
        builder = new WrapperQueryBuilder("{ \"match_all\" : {\"_name\" : \"foobar\"}}").queryName("outer");
        assertEquals(new BoolQueryBuilder().must(new MatchAllQueryBuilder().queryName("foobar")).queryName("outer"),
            builder.rewrite(shardContext));
    }

    public void testRewriteWithInnerBoost() throws IOException {
        final TermQueryBuilder query = new TermQueryBuilder("foo", "bar").boost(2);
        QueryBuilder builder = new WrapperQueryBuilder(query.toString());
        QueryShardContext shardContext = createShardContext();
        assertEquals(query, builder.rewrite(shardContext));
        builder = new WrapperQueryBuilder(query.toString()).boost(3);
        assertEquals(new BoolQueryBuilder().must(query).boost(3), builder.rewrite(shardContext));
    }

    @Override
    protected Query rewrite(Query query) throws IOException {
        // WrapperQueryBuilder adds some optimization if the wrapper and query builder have boosts / query names that wraps
        // the actual QueryBuilder that comes from the binary blob into a BooleanQueryBuilder to give it an outer boost / name
        // this causes some queries to be not exactly equal but equivalent such that we need to rewrite them before comparing.
        if (query != null) {
            MemoryIndex idx = new MemoryIndex();
            return idx.createSearcher().rewrite(query);
        }
        return new MatchAllDocsQuery(); // null == *:*
    }

}
