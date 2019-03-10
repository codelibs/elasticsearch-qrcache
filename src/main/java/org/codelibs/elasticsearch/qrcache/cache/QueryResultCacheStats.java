package org.codelibs.elasticsearch.qrcache.cache;

import java.io.IOException;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class QueryResultCacheStats implements Streamable, ToXContent {

    long size;

    long requestMemorySize;

    long responseMemorySize;

    long hits;

    long evictions;

    long total;

    public QueryResultCacheStats() {
    }

    public QueryResultCacheStats(final long size, final long requestMemorySize, final long responseMemorySize, final long total,
            final long hits, final long evictions) {
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
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(Fields.QUERY_CACHE_STATS.getPreferredName());
        builder.field(Fields.SIZE.getPreferredName(), getSize());
        builder.field(Fields.REQUEST_MEMORY_SIZE_IN_BYTES.getPreferredName(), requestMemorySize);
        builder.field(Fields.RESPONSE_MEMORY_SIZE_IN_BYTES.getPreferredName(), responseMemorySize);
        builder.field(Fields.TOTAL.getPreferredName(), getTotal());
        builder.field(Fields.HITS.getPreferredName(), getHits());
        builder.field(Fields.EVICTIONS.getPreferredName(), getEvictions());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final ParseField QUERY_CACHE_STATS = new ParseField("query_result_cache");

        static final ParseField SIZE = new ParseField("size");

        static final ParseField REQUEST_MEMORY_SIZE = new ParseField("request_memory_size");

        static final ParseField REQUEST_MEMORY_SIZE_IN_BYTES = new ParseField("request_memory_size_in_bytes");

        static final ParseField RESPONSE_MEMORY_SIZE = new ParseField("response_memory_size");

        static final ParseField RESPONSE_MEMORY_SIZE_IN_BYTES = new ParseField("response_memory_size_in_bytes");

        static final ParseField TOTAL = new ParseField("total");

        static final ParseField HITS = new ParseField("hits");

        static final ParseField EVICTIONS = new ParseField("evictions");
    }
}