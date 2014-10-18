package org.codelibs.elasticsearch.qrcache.cache;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

public class QueryResultCacheStats implements Streamable, ToXContent {

    long size;

    long requestMemorySize;

    long responseMemorySize;

    long hits;

    long evictions;

    long total;

    public QueryResultCacheStats() {
    }

    public QueryResultCacheStats(final long size, final long requestMemorySize,
            final long responseMemorySize, final long total, final long hits,
            final long evictions) {
        this.size = size;
        this.requestMemorySize = requestMemorySize;
        this.responseMemorySize = responseMemorySize;
        this.total = total;
        this.hits = hits;
        this.evictions = evictions;
    }

    public void add(final QueryResultCacheStats stats) {
        size += stats.size;
        requestMemorySize += stats.requestMemorySize;
        responseMemorySize += stats.responseMemorySize;
        total += stats.total;
        hits += stats.hits;
        evictions += stats.evictions;
    }

    public long getSize() {
        return size;
    }

    public long getRequestMemorySizeInBytes() {
        return requestMemorySize;
    }

    public long getResponseMemorySizeInBytes() {
        return requestMemorySize;
    }

    public ByteSizeValue getRequestMemorySize() {
        return new ByteSizeValue(requestMemorySize);
    }

    public ByteSizeValue getResponseMemorySize() {
        return new ByteSizeValue(responseMemorySize);
    }

    public long getEvictions() {
        return evictions;
    }

    public long getHits() {
        return hits;
    }

    public long getTotal() {
        return total;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        size = in.readVLong();
        requestMemorySize = in.readVLong();
        responseMemorySize = in.readVLong();
        total = in.readVLong();
        hits = in.readVLong();
        evictions = in.readVLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVLong(size);
        out.writeVLong(requestMemorySize);
        out.writeVLong(responseMemorySize);
        out.writeVLong(total);
        out.writeVLong(hits);
        out.writeVLong(evictions);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder,
            final Params params) throws IOException {
        builder.startObject(Fields.QUERY_CACHE_STATS);
        builder.field(Fields.SIZE, getSize());
        builder.byteSizeField(Fields.REQUEST_MEMORY_SIZE_IN_BYTES,
                Fields.REQUEST_MEMORY_SIZE, requestMemorySize);
        builder.byteSizeField(Fields.RESPONSE_MEMORY_SIZE_IN_BYTES,
                Fields.RESPONSE_MEMORY_SIZE, responseMemorySize);
        builder.field(Fields.TOTAL, getTotal());
        builder.field(Fields.HITS, getHits());
        builder.field(Fields.EVICTIONS, getEvictions());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString QUERY_CACHE_STATS = new XContentBuilderString(
                "query_result_cache");

        static final XContentBuilderString SIZE = new XContentBuilderString(
                "size");

        static final XContentBuilderString REQUEST_MEMORY_SIZE = new XContentBuilderString(
                "request_memory_size");

        static final XContentBuilderString REQUEST_MEMORY_SIZE_IN_BYTES = new XContentBuilderString(
                "request_memory_size_in_bytes");

        static final XContentBuilderString RESPONSE_MEMORY_SIZE = new XContentBuilderString(
                "response_memory_size");

        static final XContentBuilderString RESPONSE_MEMORY_SIZE_IN_BYTES = new XContentBuilderString(
                "response_memory_size_in_bytes");

        static final XContentBuilderString TOTAL = new XContentBuilderString(
                "total");

        static final XContentBuilderString HITS = new XContentBuilderString(
                "hits");

        static final XContentBuilderString EVICTIONS = new XContentBuilderString(
                "evictions");
    }
}