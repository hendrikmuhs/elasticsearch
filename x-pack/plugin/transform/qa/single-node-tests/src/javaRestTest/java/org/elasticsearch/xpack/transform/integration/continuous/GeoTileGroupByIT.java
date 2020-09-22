package org.elasticsearch.xpack.transform.integration.continuous;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class GeoTileGroupByIT extends ContinuousTestCase {
    private static final String NAME = "continuous-geo-tile-pivot-test";
    private static final String MISSING_BUCKET_KEY = "0,0";

    private final boolean missing;
    private final int precision;

    public GeoTileGroupByIT() {
        missing = false; //randomBoolean();
        // limit precision to max 10 to avoid long runtime
        precision = 8; //randomIntBetween(1, 10);
    }

    @Override
    public TransformConfig createConfig() {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder();
        addCommonBuilderParameters(transformConfigBuilder);
        transformConfigBuilder.setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX));
        transformConfigBuilder.setDest(new DestConfig(NAME, INGEST_PIPELINE));
        transformConfigBuilder.setId(NAME);
        PivotConfig.Builder pivotConfigBuilder = new PivotConfig.Builder();
        pivotConfigBuilder.setGroups(
            new GroupConfig.Builder().groupBy(
                "tile",
                new GeoTileGroupSource.Builder().setField("location").setMissingBucket(missing).setPrecision(precision).build()
            ).build()
        );

        AggregatorFactories.Builder aggregations = new AggregatorFactories.Builder();
        addCommonAggregations(aggregations);

        pivotConfigBuilder.setAggregations(aggregations);
        transformConfigBuilder.setPivotConfig(pivotConfigBuilder.build());
        return transformConfigBuilder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void testIteration(int iteration) throws IOException {
        SearchRequest searchRequestSource = new SearchRequest(CONTINUOUS_EVENTS_SOURCE_INDEX).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilderSource = new SearchSourceBuilder().size(0);
        GeoGridAggregationBuilder byLocation = new GeoTileGridAggregationBuilder("tile").field("location").precision(precision);

        if (missing) {
            // missing_bucket produces `null`, we can't use `null` in aggs, so we have to use a magic value, see gh#60043
            byLocation.missing(MISSING_BUCKET_KEY);
        }
        sourceBuilderSource.aggregation(byLocation);
        searchRequestSource.source(sourceBuilderSource);
        SearchResponse responseSource = search(searchRequestSource);

        SearchRequest searchRequestDest = new SearchRequest(NAME).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilderDest = new SearchSourceBuilder().size(10_000);
        searchRequestDest.source(sourceBuilderDest);
        SearchResponse responseDest = search(searchRequestDest);

        List<Tuple<Rectangle, Long>> pointsAggregated = ((GeoGrid) responseSource.getAggregations().get("tile")).getBuckets().stream().map(b-> {

            return new Tuple<>(GeoTileUtils.toBoundingBox(b.getKeyAsString()), b.getDocCount());
        }).collect(Collectors.toList());

        pointsAggregated.sort(
            Comparator.comparing((Tuple<Rectangle, Long> p) -> p.v1().getMinY()).thenComparing(Comparator.comparing(p -> p.v1().getMinX()))
        );

        List<Tuple<Rectangle, Long>> pointsTransformed = Arrays.stream(responseDest.getHits().getHits()).map(s -> {
            Map<String, Object> tileObj = (Map<String, Object>) XContentMapValues.extractValue("tile", s.getSourceAsMap());
            List<List<Double>> coordinates = ((List<List<List<Double>>>) tileObj.get("coordinates")).get(0);

            // TODO: this should be a long
            Double count = (Double) XContentMapValues.extractValue("count", s.getSourceAsMap());

            Rectangle bBox = new Rectangle(coordinates.get(1).get(0), coordinates.get(0).get(0),
                coordinates.get(2).get(1), coordinates.get(0).get(1)
                );


//            double lon = (coordinates.get(0).get(0) + coordinates.get(2).get(0)) / 2;
//            double lat = (coordinates.get(0).get(1) + coordinates.get(2).get(1)) / 2;
//            logger.info("lon {} {} {}",  coordinates.get(0).get(0), coordinates.get(2).get(0), lon  );
//            logger.info("lat {} {} {}",  coordinates.get(0).get(1), coordinates.get(2).get(1), lat  );

            return new Tuple<Rectangle, Long>(bBox, count.longValue());
        })
            .sorted(
                Comparator.comparing((Tuple<Rectangle, Long> p) -> p.v1().getMinY()).thenComparing(Comparator.comparing(p -> p.v1().getMinX()))
            )
            .collect(Collectors.toList());

        /*assertEquals(
            "Number of buckets did not match, source: "
                + pointsTransformed.size()
                + ", expected: "
                + pointsAggregated.size()
                + ", iteration: "
                + iteration
                + " "
                + pointsTransformed
                + " / "
                + pointsAggregated

                ,
            pointsAggregated.size(),
            pointsTransformed.size()
        );*/

        logger.info("iteration {} \n\n{}\n----\n{}", iteration, pointsTransformed, pointsAggregated);

        Iterator<Tuple<Rectangle, Long>> sourceIterator = pointsAggregated.iterator();
        Iterator<Tuple<Rectangle, Long>> destIterator = pointsTransformed.iterator();
        int i = 0;

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            Tuple<Rectangle, Long> searchValue = sourceIterator.next();
            Tuple<Rectangle, Long> transformValue = destIterator.next();

            assertEquals(
                "Doc count did not match, source: " + transformValue.v2() + ", expected: " + searchValue.v2() + ", iteration: " + iteration + ", i: " + i,
                transformValue.v2(),
                searchValue.v2()
            );

            assertThat(
                "Buckets did not match, source: " + transformValue.v1() + ", expected: " + searchValue.v1() + ", iteration: " + iteration,
                transformValue.v1(),
                equalTo(searchValue.v1())
            );

            ++i;
        }
        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }
}
