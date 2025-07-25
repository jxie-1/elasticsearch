/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Matcher;
import org.junit.Rule;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Creates indices with many different mappings and fetches values from them to make sure
 * we can do it. Think of this as an integration test for {@link BlockLoader}
 * implementations <strong>and</strong> an integration test for field resolution.
 * This is a port of a test with the same name on the SQL side.
 */
public abstract class FieldExtractorTestCase extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(FieldExtractorTestCase.class);

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @ParametersFactory(argumentFormatting = "%s")
    public static List<Object[]> args() throws Exception {
        return List.of(
            new Object[] { null },
            new Object[] { MappedFieldType.FieldExtractPreference.NONE },
            new Object[] { MappedFieldType.FieldExtractPreference.STORED }
        );
    }

    protected final MappedFieldType.FieldExtractPreference preference;

    protected FieldExtractorTestCase(MappedFieldType.FieldExtractPreference preference) {
        this.preference = preference;
    }

    public void testTextField() throws IOException {
        textTest().test(randomAlphaOfLength(20));
    }

    private Test textTest() {
        return new Test("text").randomStoreUnlessSynthetic();
    }

    public void testKeywordField() throws IOException {
        Integer ignoreAbove = randomBoolean() ? null : between(10, 50);
        int length = between(10, 50);

        String value = randomAlphaOfLength(length);
        keywordTest().ignoreAbove(ignoreAbove).test(value, ignoredByIgnoreAbove(ignoreAbove, length) ? null : value);
    }

    private Test keywordTest() {
        return new Test("keyword").randomDocValuesAndStoreUnlessSynthetic();
    }

    public void testConstantKeywordField() throws IOException {
        boolean specifyInMapping = randomBoolean();
        boolean specifyInDocument = randomBoolean();

        String value = randomAlphaOfLength(20);
        new Test("constant_keyword").expectedType("keyword")
            .value(specifyInMapping ? value : null)
            .test(specifyInDocument ? value : null, specifyInMapping || specifyInDocument ? value : null);
    }

    public void testWildcardField() throws IOException {
        Integer ignoreAbove = randomBoolean() ? null : between(10, 50);
        int length = between(10, 50);

        String value = randomAlphaOfLength(length);
        new Test("wildcard").expectedType("keyword")
            .ignoreAbove(ignoreAbove)
            .test(value, ignoredByIgnoreAbove(ignoreAbove, length) ? null : value);
    }

    public void testLong() throws IOException {
        long value = randomLong();
        longTest().test(randomBoolean() ? Long.toString(value) : value, value);
    }

    public void testLongWithDecimalParts() throws IOException {
        long value = randomLong();
        int decimalPart = between(1, 99);
        BigDecimal withDecimals = new BigDecimal(value + "." + decimalPart);
        /*
         * It's possible to pass the BigDecimal here without converting to a string
         * but that rounds in a different way, and I'm not quite able to reproduce it
         * at the time.
         */
        longTest().test(withDecimals.toString(), value);
    }

    public void testLongMalformed() throws IOException {
        longTest().forceIgnoreMalformed().test(randomAlphaOfLength(5), null);
    }

    private Test longTest() {
        return new Test("long").randomIgnoreMalformedUnlessSynthetic().randomDocValuesUnlessSynthetic();
    }

    public void testInt() throws IOException {
        int value = randomInt();
        intTest().test(randomBoolean() ? Integer.toString(value) : value, value);
    }

    public void testIntWithDecimalParts() throws IOException {
        double value = randomDoubleBetween(Integer.MIN_VALUE, Integer.MAX_VALUE, true);
        intTest().test(randomBoolean() ? Double.toString(value) : value, (int) value);
    }

    public void testIntMalformed() throws IOException {
        intTest().forceIgnoreMalformed().test(randomAlphaOfLength(5), null);
    }

    private Test intTest() {
        return new Test("integer").randomIgnoreMalformedUnlessSynthetic().randomDocValuesUnlessSynthetic();
    }

    public void testShort() throws IOException {
        short value = randomShort();
        shortTest().test(randomBoolean() ? Short.toString(value) : value, (int) value);
    }

    public void testShortWithDecimalParts() throws IOException {
        double value = randomDoubleBetween(Short.MIN_VALUE, Short.MAX_VALUE, true);
        shortTest().test(randomBoolean() ? Double.toString(value) : value, (int) value);
    }

    public void testShortMalformed() throws IOException {
        shortTest().forceIgnoreMalformed().test(randomAlphaOfLength(5), null);
    }

    private Test shortTest() {
        return new Test("short").expectedType("integer").randomIgnoreMalformedUnlessSynthetic().randomDocValuesUnlessSynthetic();
    }

    public void testByte() throws IOException {
        byte value = randomByte();
        byteTest().test(Byte.toString(value), (int) value);
    }

    public void testByteWithDecimalParts() throws IOException {
        double value = randomDoubleBetween(Byte.MIN_VALUE, Byte.MAX_VALUE, true);
        byteTest().test(randomBoolean() ? Double.toString(value) : value, (int) value);
    }

    public void testByteMalformed() throws IOException {
        byteTest().forceIgnoreMalformed().test(randomAlphaOfLength(5), null);
    }

    private Test byteTest() {
        return new Test("byte").expectedType("integer").randomIgnoreMalformedUnlessSynthetic().randomDocValuesUnlessSynthetic();
    }

    public void testUnsignedLong() throws IOException {
        assumeTrue(
            "order of fields in error message inconsistent before 8.14",
            getCachedNodesVersions().stream().allMatch(v -> Version.fromString(v).onOrAfter(Version.V_8_14_0))
        );
        BigInteger value = randomUnsignedLong();
        new Test("unsigned_long").randomIgnoreMalformedUnlessSynthetic()
            .randomDocValuesUnlessSynthetic()
            .test(
                randomBoolean() ? value.toString() : value,
                value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0 ? value.longValue() : value
            );
    }

    public void testUnsignedLongMalformed() throws IOException {
        new Test("unsigned_long").forceIgnoreMalformed().randomDocValuesUnlessSynthetic().test(randomAlphaOfLength(5), null);
    }

    public void testDouble() throws IOException {
        double value = randomDouble();
        new Test("double").randomIgnoreMalformedUnlessSynthetic()
            .randomDocValuesUnlessSynthetic()
            .test(randomBoolean() ? Double.toString(value) : value, value);
    }

    public void testFloat() throws IOException {
        float value = randomFloat();
        new Test("float").expectedType("double")
            .randomIgnoreMalformedUnlessSynthetic()
            .randomDocValuesUnlessSynthetic()
            .test(randomBoolean() ? Float.toString(value) : value, (double) value);
    }

    public void testScaledFloat() throws IOException {
        // Running this on 17 when nodes in cluster run JDK >17 triggers an assert due to a mismatch
        // of results produced by Double#toString for some specific numbers.
        // See https://github.com/elastic/elasticsearch/issues/122984.
        assumeTrue("JDK version greater than 17", Runtime.version().feature() > 17);

        double value = randomBoolean() ? randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true) : randomFloat();
        // Scale factors less than about 5.6e-309 will result in NaN (due to 1/scaleFactor being infinity)
        double scalingFactor = randomDoubleBetween(1e-308, Double.MAX_VALUE, false);
        new Test("scaled_float").expectedType("double")
            .randomIgnoreMalformedUnlessSynthetic()
            .randomDocValuesUnlessSynthetic()
            .scalingFactor(scalingFactor)
            .test(randomBoolean() ? Double.toString(value) : value, scaledFloatMatcher(scalingFactor, value));
    }

    private Matcher<Double> scaledFloatMatcher(double scalingFactor, double d) {
        long encoded = Math.round(d * scalingFactor);
        double decoded = encoded / scalingFactor;
        // We can lose a little more the ulp in the round trip.
        return closeTo(decoded, Math.ulp(decoded) * 2);
    }

    public void testBoolean() throws IOException {
        boolean value = randomBoolean();
        new Test("boolean").ignoreMalformed(randomBoolean())
            .randomDocValuesUnlessSynthetic()
            .test(randomBoolean() ? Boolean.toString(value) : value, value);
    }

    public void testIp() throws IOException {
        ipTest().test(NetworkAddress.format(randomIp(randomBoolean())));
    }

    private Test ipTest() {
        return new Test("ip").ignoreMalformed(randomBoolean());
    }

    public void testVersionField() throws IOException {
        new Test("version").test(randomVersionString());
    }

    public void testGeoPoint() throws IOException {
        assumeTrue(
            "not supported until 8.13",
            getCachedNodesVersions().stream().allMatch(v -> Version.fromString(v).onOrAfter(Version.V_8_13_0))
        );
        new Test("geo_point")
            // TODO we should support loading geo_point from doc values if source isn't enabled
            .sourceMode(randomValueOtherThanMany(s -> s.stored() == false, () -> randomFrom(SourceMode.values())))
            .ignoreMalformed(randomBoolean())
            .storeAndDocValues(randomBoolean(), randomBoolean())
            .test(GeometryTestUtils.randomPoint(false).toString());
    }

    public void testGeoShape() throws IOException {
        assumeTrue(
            "not supported until 8.13",
            getCachedNodesVersions().stream().allMatch(v -> Version.fromString(v).onOrAfter(Version.V_8_13_0))
        );
        new Test("geo_shape")
            // TODO if source isn't enabled how can we load *something*? It's just triangles, right?
            .sourceMode(randomValueOtherThanMany(s -> s.stored() == false, () -> randomFrom(SourceMode.values())))
            .ignoreMalformed(randomBoolean())
            .storeAndDocValues(randomBoolean(), randomBoolean())
            // TODO pick supported random shapes
            .test(GeometryTestUtils.randomPoint(false).toString());
    }

    public void testAliasToKeyword() throws IOException {
        keywordTest().createAlias().test(randomAlphaOfLength(20));
    }

    public void testAliasToText() throws IOException {
        textTest().createAlias().test(randomAlphaOfLength(20));
    }

    public void testAliasToInt() throws IOException {
        intTest().createAlias().test(randomInt());
    }

    public void testFlattenedUnsupported() throws IOException {
        assumeOriginalTypesReported();
        new Test("flattened").createIndex("test", "flattened");
        index("test", """
            {"flattened": {"a": "foo"}}""");
        Map<String, Object> result = runEsql("FROM test* | LIMIT 2");

        assertResultMap(result, List.of(unsupportedColumnInfo("flattened", "flattened")), List.of(matchesList().item(null)));
    }

    public void testEmptyMapping() throws IOException {
        createIndex("test", index -> {});
        index("test", """
            {}""");

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT missing | LIMIT 3"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(err, containsString("Unknown column [missing]"));

        // TODO this is broken in main too
        // Map<String, Object> result = runEsqlSync(
        // new RestEsqlTestCase.RequestObjectBuilder().query("FROM test* | LIMIT 2"),
        // new AssertWarnings.NoWarnings(),
        // profileLogger
        // );
        // assertResultMap(
        // result,
        // matchesMap().entry("columns", List.of(columnInfo("f", "unsupported"), columnInfo("f.raw", "unsupported")))
        // .entry("values", List.of(matchesList().item(null).item(null)))
        // );
    }

    /**
     * <pre>
     * "text_field": {
     *   "type": "text",
     *   "fields": {
     *     "raw": {
     *       "type": "keyword",
     *       "ignore_above": 10
     *     }
     *   }
     * }
     * </pre>
     */
    public void testTextFieldWithKeywordSubfield() throws IOException {
        String value = randomAlphaOfLength(20);
        Map<String, Object> result = new Test("text").storeAndDocValues(randomBoolean(), null).sub("raw", keywordTest()).roundTrip(value);
        assertResultMap(
            result,
            List.of(columnInfo("text_field", "text"), columnInfo("text_field.raw", "keyword")),
            List.of(matchesList().item(value).item(value))
        );
    }

    /**
     * <pre>
     * "text_field": {
     *   "type": "text",
     *   "fields": {
     *     "int": {
     *       "type": "integer",
     *       "ignore_malformed": true/false
     *     }
     *   }
     * }
     * </pre>
     */
    public void testTextFieldWithIntegerSubfield() throws IOException {
        int value = randomInt();
        Map<String, Object> result = textTest().sub("int", intTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("text_field", "text"), columnInfo("text_field.int", "integer")),
            List.of(matchesList().item(Integer.toString(value)).item(value))
        );
    }

    /**
     * <pre>
     * "text_field": {
     *   "type": "text",
     *   "fields": {
     *     "int": {
     *       "type": "integer",
     *       "ignore_malformed": true
     *     }
     *   }
     * }
     * </pre>
     */
    public void testTextFieldWithIntegerSubfieldMalformed() throws IOException {
        String value = randomAlphaOfLength(5);
        Map<String, Object> result = textTest().sourceMode(SourceMode.DEFAULT).sub("int", intTest().ignoreMalformed(true)).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("text_field", "text"), columnInfo("text_field.int", "integer")),
            List.of(matchesList().item(value).item(null))
        );
    }

    /**
     * <pre>
     * "text_field": {
     *   "type": "text",
     *   "fields": {
     *     "ip": {
     *       "type": "ip",
     *       "ignore_malformed": true/false
     *     }
     *   }
     * }
     * </pre>
     */
    public void testTextFieldWithIpSubfield() throws IOException {
        String value = NetworkAddress.format(randomIp(randomBoolean()));
        Map<String, Object> result = textTest().sub("ip", ipTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("text_field", "text"), columnInfo("text_field.ip", "ip")),
            List.of(matchesList().item(value).item(value))
        );
    }

    /**
     * <pre>
     * "text_field": {
     *   "type": "text",
     *   "fields": {
     *     "ip": {
     *       "type": "ip",
     *       "ignore_malformed": true
     *     }
     *   }
     * }
     * </pre>
     */
    public void testTextFieldWithIpSubfieldMalformed() throws IOException {
        String value = randomAlphaOfLength(10);
        Map<String, Object> result = textTest().sourceMode(SourceMode.DEFAULT).sub("ip", ipTest().ignoreMalformed(true)).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("text_field", "text"), columnInfo("text_field.ip", "ip")),
            List.of(matchesList().item(value).item(null))
        );
    }

    /**
     * <pre>
     * "integer_field": {
     *   "type": "integer",
     *   "ignore_malformed": true/false,
     *   "fields": {
     *     "str": {
     *       "type": "text/keyword"
     *     }
     *   }
     * }
     * </pre>
     */
    public void testIntFieldWithTextOrKeywordSubfield() throws IOException {
        int value = randomInt();
        boolean text = randomBoolean();
        Map<String, Object> result = intTest().sub("str", text ? textTest() : keywordTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("integer_field", "integer"), columnInfo("integer_field.str", text ? "text" : "keyword")),
            List.of(matchesList().item(value).item(Integer.toString(value)))
        );
    }

    /**
     * <pre>
     * "integer_field": {
     *   "type": "integer",
     *   "ignore_malformed": true,
     *   "fields": {
     *     "str": {
     *       "type": "text/keyword"
     *     }
     *   }
     * }
     * </pre>
     */
    public void testIntFieldWithTextOrKeywordSubfieldMalformed() throws IOException {
        String value = randomAlphaOfLength(5);
        boolean text = randomBoolean();
        Map<String, Object> result = intTest().forceIgnoreMalformed().sub("str", text ? textTest() : keywordTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("integer_field", "integer"), columnInfo("integer_field.str", text ? "text" : "keyword")),
            List.of(matchesList().item(null).item(value))
        );
    }

    /**
     * <pre>
     * "ip_field": {
     *   "type": "ip",
     *   "ignore_malformed": true/false,
     *   "fields": {
     *     "str": {
     *       "type": "text/keyword"
     *     }
     *   }
     * }
     * </pre>
     */
    public void testIpFieldWithTextOrKeywordSubfield() throws IOException {
        String value = NetworkAddress.format(randomIp(randomBoolean()));
        boolean text = randomBoolean();
        Map<String, Object> result = ipTest().sub("str", text ? textTest() : keywordTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("ip_field", "ip"), columnInfo("ip_field.str", text ? "text" : "keyword")),
            List.of(matchesList().item(value).item(value))
        );
    }

    /**
     * <pre>
     * "ip_field": {
     *   "type": "ip",
     *   "ignore_malformed": true,
     *   "fields": {
     *     "str": {
     *       "type": "text/keyword"
     *     }
     *   }
     * }
     * </pre>
     */
    public void testIpFieldWithTextOrKeywordSubfieldMalformed() throws IOException {
        String value = randomAlphaOfLength(5);
        boolean text = randomBoolean();
        Map<String, Object> result = ipTest().forceIgnoreMalformed().sub("str", text ? textTest() : keywordTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("ip_field", "ip"), columnInfo("ip_field.str", text ? "text" : "keyword")),
            List.of(matchesList().item(null).item(value))
        );
    }

    /**
     * <pre>
     * "integer_field": {
     *   "type": "ip",
     *   "ignore_malformed": true/false,
     *   "fields": {
     *     "byte": {
     *       "type": "byte",
     *       "ignore_malformed": true/false
     *     }
     *   }
     * }
     * </pre>
     */
    public void testIntFieldWithByteSubfield() throws IOException {
        byte value = randomByte();
        Map<String, Object> result = intTest().sub("byte", byteTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("integer_field", "integer"), columnInfo("integer_field.byte", "integer")),
            List.of(matchesList().item((int) value).item((int) value))
        );
    }

    /**
     * <pre>
     * "integer_field": {
     *   "type": "integer",
     *   "ignore_malformed": true/false,
     *   "fields": {
     *     "byte": {
     *       "type": "byte",
     *       "ignore_malformed": true
     *     }
     *   }
     * }
     * </pre>
     */
    public void testIntFieldWithByteSubfieldTooBig() throws IOException {
        int value = randomValueOtherThanMany((Integer v) -> (Byte.MIN_VALUE <= v) && (v <= Byte.MAX_VALUE), ESTestCase::randomInt);
        Map<String, Object> result = intTest().sourceMode(SourceMode.DEFAULT)
            .sub("byte", byteTest().ignoreMalformed(true))
            .roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("integer_field", "integer"), columnInfo("integer_field.byte", "integer")),
            List.of(matchesList().item(value).item(null))
        );
    }

    /**
     * <pre>
     * "byte_field": {
     *   "type": "byte",
     *   "ignore_malformed": true/false,
     *   "fields": {
     *     "int": {
     *       "type": "int",
     *       "ignore_malformed": true/false
     *     }
     *   }
     * }
     * </pre>
     */
    public void testByteFieldWithIntSubfield() throws IOException {
        byte value = randomByte();
        Map<String, Object> result = byteTest().sub("int", intTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("byte_field", "integer"), columnInfo("byte_field.int", "integer")),
            List.of(matchesList().item((int) value).item((int) value))
        );
    }

    /**
     * <pre>
     * "byte_field": {
     *   "type": "byte",
     *   "ignore_malformed": true,
     *   "fields": {
     *     "int": {
     *       "type": "int",
     *       "ignore_malformed": true/false
     *     }
     *   }
     * }
     * </pre>
     */
    public void testByteFieldWithIntSubfieldTooBig() throws IOException {
        int value = randomValueOtherThanMany((Integer v) -> (Byte.MIN_VALUE <= v) && (v <= Byte.MAX_VALUE), ESTestCase::randomInt);
        Map<String, Object> result = byteTest().forceIgnoreMalformed().sub("int", intTest()).roundTrip(value);

        assertResultMap(
            result,
            List.of(columnInfo("byte_field", "integer"), columnInfo("byte_field.int", "integer")),
            List.of(matchesList().item(null).item(value))
        );
    }

    /**
     * Two indices, one with:
     * <pre>
     * "f": {
     *     "type": "keyword"
     * }
     * </pre>
     * and the other with
     * <pre>
     * "f": {
     *     "type": "long"
     * }
     * </pre>.
     */
    public void testIncompatibleTypes() throws IOException {
        assumeSuggestedCastReported();
        keywordTest().createIndex("test1", "f");
        index("test1", """
            {"f": "f1"}""");
        longTest().createIndex("test2", "f");
        index("test2", """
            {"f": 1}""");

        Map<String, Object> result = runEsql("FROM test*");
        assertResultMap(
            result,
            List.of(unsupportedColumnInfo("f", "keyword", "long")),
            List.of(matchesList().item(null), matchesList().item(null))
        );
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT f | LIMIT 3"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(
            deyaml(err),
            containsString(
                "Cannot use field [f] due to ambiguities being mapped as [2] incompatible types: [keyword] in [test1], [long] in [test2]"
            )
        );
    }

    /**
     * Two indices, one with:
     * <pre>
     * "file": {
     *     "type": "keyword"
     * }
     * </pre>
     * and the other with
     * <pre>
     * "other_file": {
     *     "type": "keyword"
     * }
     * </pre>.
     */
    public void testDistinctInEachIndex() throws IOException {
        keywordTest().createIndex("test1", "file");
        index("test1", """
            {"file": "f1"}""");
        keywordTest().createIndex("test2", "other");
        index("test2", """
            {"other": "o2"}""");

        Map<String, Object> result = runEsql("FROM test* | SORT file, other");
        assertResultMap(
            result,
            List.of(columnInfo("file", "keyword"), columnInfo("other", "keyword")),
            List.of(matchesList().item("f1").item(null), matchesList().item(null).item("o2"))
        );
    }

    /**
     * Two indices, one with:
     * <pre>
     * "file": {
     *    "type": "keyword"
     * }
     * </pre>
     * and the other with
     * <pre>
     * "file": {
     *    "type": "object",
     *    "properties": {
     *       "raw": {
     *          "type": "keyword"
     *       }
     *    }
     * }
     * </pre>.
     */
    public void testMergeKeywordAndObject() throws IOException {
        assumeSuggestedCastReported();
        keywordTest().createIndex("test1", "file");
        index("test1", """
            {"file": "f1"}""");
        createIndex("test2", index -> {
            index.startObject("properties");
            {
                index.startObject("file");
                {
                    index.field("type", "object");
                    index.startObject("properties");
                    {
                        index.startObject("raw").field("type", "keyword").endObject();
                    }
                    index.endObject();
                }
                index.endObject();
            }
            index.endObject();
        });
        index("test2", """
            {"file": {"raw": "o2"}}""");

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT file, file.raw | LIMIT 3"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(
            deyaml(err),
            containsString(
                "Cannot use field [file] due to ambiguities"
                    + " being mapped as [2] incompatible types: [keyword] in [test1], [object] in [test2]"
            )
        );

        Map<String, Object> result = runEsql("FROM test* | SORT file.raw | LIMIT 2");
        assertResultMap(
            result,
            List.of(unsupportedColumnInfo("file", "keyword", "object"), columnInfo("file.raw", "keyword")),
            List.of(matchesList().item(null).item("o2"), matchesList().item(null).item(null))
        );
    }

    /**
     * One index with an unsupported field and a supported sub-field. The supported sub-field
     * is marked as unsupported <strong>because</strong> the parent is unsupported. Mapping:
     * <pre>
     * "f": {
     *    "type": "ip_range"  ----- The type here doesn't matter, but it has to be one we don't support
     *    "fields": {
     *       "raw": {
     *          "type": "keyword"
     *       }
     *    }
     * }
     * </pre>.
     */
    public void testPropagateUnsupportedToSubFields() throws IOException {
        assumeOriginalTypesReported();
        createIndex("test", index -> {
            index.startObject("properties");
            index.startObject("f");
            {
                index.field("type", "ip_range");
                index.startObject("fields");
                {
                    index.startObject("raw").field("type", "keyword").endObject();
                }
                index.endObject();
            }
            index.endObject();
            index.endObject();
        });
        index("test", """
            {"f": "192.168.0.1/24"}""");

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT f, f.raw | LIMIT 3"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(err, containsString("Cannot use field [f] with unsupported type [ip_range]"));
        assertThat(err, containsString("Cannot use field [f.raw] with unsupported type [ip_range]"));

        Map<String, Object> result = runEsql("FROM test* | LIMIT 2");
        assertResultMap(
            result,
            List.of(unsupportedColumnInfo("f", "ip_range"), unsupportedColumnInfo("f.raw", "ip_range")),
            List.of(matchesList().item(null).item(null))
        );
    }

    /**
     * Two indices, one with:
     * <pre>
     * "f": {
     *    "type": "ip_range"  ----- The type here doesn't matter, but it has to be one we don't support
     * }
     * </pre>
     * and the other with
     * <pre>
     * "f": {
     *    "type": "object",
     *    "properties": {
     *       "raw": {
     *          "type": "keyword"
     *       }
     *    }
     * }
     * </pre>.
     */
    public void testMergeUnsupportedAndObject() throws IOException {
        assumeOriginalTypesReported();
        createIndex("test1", index -> {
            index.startObject("properties");
            index.startObject("f").field("type", "ip_range").endObject();
            index.endObject();
        });
        index("test1", """
            {"f": "192.168.0.1/24"}""");
        createIndex("test2", index -> {
            index.startObject("properties");
            {
                index.startObject("f");
                {
                    index.field("type", "object");
                    index.startObject("properties");
                    {
                        index.startObject("raw").field("type", "keyword").endObject();
                    }
                    index.endObject();
                }
                index.endObject();
            }
            index.endObject();
        });
        index("test2", """
            {"f": {"raw": "o2"}}""");

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT f, f.raw | LIMIT 3"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(err, containsString("Cannot use field [f] with unsupported type [ip_range]"));
        assertThat(err, containsString("Cannot use field [f.raw] with unsupported type [ip_range]"));

        Map<String, Object> result = runEsql("FROM test* | LIMIT 2");
        assertResultMap(
            result,
            List.of(unsupportedColumnInfo("f", "ip_range"), unsupportedColumnInfo("f.raw", "ip_range")),
            List.of(matchesList().item(null).item(null), matchesList().item(null).item(null))
        );
    }

    /**
     * Two indices, one with:
     * <pre>
     * "emp_no": {
     *     "type": "integer"
     * }
     * </pre>
     * and the other with
     * <pre>
     * "emp_no": {
     *     "type": "integer",
     *     "doc_values": false
     * }
     * </pre>.
     */
    public void testIntegerDocValuesConflict() throws IOException {
        assumeTrue(
            "order of fields in error message inconsistent before 8.14",
            getCachedNodesVersions().stream().allMatch(v -> Version.fromString(v).onOrAfter(Version.V_8_14_0))
        );
        intTest().sourceMode(SourceMode.DEFAULT).storeAndDocValues(null, true).createIndex("test1", "emp_no");
        index("test1", """
            {"emp_no": 1}""");
        intTest().sourceMode(SourceMode.DEFAULT).storeAndDocValues(null, false).createIndex("test2", "emp_no");
        index("test2", """
            {"emp_no": 2}""");

        Map<String, Object> result = runEsql("FROM test* | SORT emp_no | LIMIT 2");
        assertResultMap(result, List.of(columnInfo("emp_no", "integer")), List.of(matchesList().item(1), matchesList().item(2)));
    }

    /**
     * Two indices, one with:
     * <pre>
     * "emp_no": {
     *     "type": "long"
     * }
     * </pre>
     * and the other with
     * <pre>
     * "emp_no": {
     *     "type": "integer"
     * }
     * </pre>.
     *
     * In an ideal world we'd promote the {@code integer} to an {@code long} and just go.
     */
    public void testLongIntegerConflict() throws IOException {
        assumeSuggestedCastReported();
        longTest().sourceMode(SourceMode.DEFAULT).createIndex("test1", "emp_no");
        index("test1", """
            {"emp_no": 1}""");
        intTest().sourceMode(SourceMode.DEFAULT).createIndex("test2", "emp_no");
        index("test2", """
            {"emp_no": 2}""");

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT emp_no | LIMIT 3"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(
            deyaml(err),
            containsString(
                "Cannot use field [emp_no] due to ambiguities being "
                    + "mapped as [2] incompatible types: [integer] in [test2], [long] in [test1]"
            )
        );

        Map<String, Object> result = runEsql("FROM test* | LIMIT 2");
        assertResultMap(
            result,
            List.of(unsupportedColumnInfo("emp_no", "integer", "long")),
            List.of(matchesList().item(null), matchesList().item(null))
        );
    }

    /**
     * Two indices, one with:
     * <pre>
     * "emp_no": {
     *     "type": "integer"
     * }
     * </pre>
     * and the other with
     * <pre>
     * "emp_no": {
     *     "type": "short"
     * }
     * </pre>.
     *
     * In an ideal world we'd promote the {@code short} to an {@code integer} and just go.
     */
    public void testIntegerShortConflict() throws IOException {
        assumeSuggestedCastReported();
        intTest().sourceMode(SourceMode.DEFAULT).createIndex("test1", "emp_no");
        index("test1", """
            {"emp_no": 1}""");
        shortTest().sourceMode(SourceMode.DEFAULT).createIndex("test2", "emp_no");
        index("test2", """
            {"emp_no": 2}""");

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT emp_no | LIMIT 3"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(
            deyaml(err),
            containsString(
                "Cannot use field [emp_no] due to ambiguities being "
                    + "mapped as [2] incompatible types: [integer] in [test1], [short] in [test2]"
            )
        );

        Map<String, Object> result = runEsql("FROM test* | LIMIT 2");
        assertResultMap(
            result,
            List.of(unsupportedColumnInfo("emp_no", "integer", "short")),
            List.of(matchesList().item(null), matchesList().item(null))
        );
    }

    /**
     * Two indices, one with:
     * <pre>
     * "foo": {
     *   "type": "object",
     *   "properties": {
     *     "emp_no": {
     *       "type": "integer"
     *     }
     * }
     * </pre>
     * and the other with
     * <pre>
     * "foo": {
     *   "type": "object",
     *   "properties": {
     *     "emp_no": {
     *       "type": "keyword"
     *     }
     * }
     * </pre>.
     */
    public void testTypeConflictInObject() throws IOException {
        assumeSuggestedCastReported();
        createIndex("test1", empNoInObject("integer"));
        index("test1", """
            {"foo": {"emp_no": 1}}""");
        createIndex("test2", empNoInObject("keyword"));
        index("test2", """
            {"foo": {"emp_no": "cat"}}""");

        Map<String, Object> result = runEsql("FROM test* | LIMIT 3");
        assertMap(
            result,
            getResultMatcher(result).entry("columns", List.of(unsupportedColumnInfo("foo.emp_no", "integer", "keyword"))).extraOk()
        );

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT foo.emp_no | LIMIT 3"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(
            deyaml(err),
            containsString(
                "Cannot use field [foo.emp_no] due to ambiguities being "
                    + "mapped as [2] incompatible types: [integer] in [test1], [keyword] in [test2]"
            )
        );
    }

    /**
     * Test for https://github.com/elastic/elasticsearch/issues/117054 fix
     */
    public void testOneNestedSubField_AndSameNameSupportedField() throws IOException {
        assumeIndexResolverNestedFieldsNameClashFixed();
        ESRestTestCase.createIndex("test", Settings.EMPTY, """
            "properties": {
              "Responses": {
                "properties": {
                  "process": {
                    "type": "nested",
                    "properties": {
                      "pid": {
                        "type": "long"
                      }
                    }
                  }
                }
              },
              "process": {
                "properties": {
                  "parent": {
                    "properties": {
                      "command_line": {
                        "type": "wildcard",
                        "fields": {
                          "text": {
                            "type": "text"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """);

        Map<String, Object> result = runEsql("FROM test");
        assertResultMap(
            result,
            List.of(columnInfo("process.parent.command_line", "keyword"), columnInfo("process.parent.command_line.text", "text")),
            Collections.EMPTY_LIST
        );

        index("test", """
            {"Responses.process.pid": 123,"process.parent.command_line":"run.bat"}""");

        result = runEsql("FROM test");
        assertResultMap(
            result,
            List.of(columnInfo("process.parent.command_line", "keyword"), columnInfo("process.parent.command_line.text", "text")),
            List.of(matchesList().item("run.bat").item("run.bat"))
        );

        result = runEsql("""
            FROM test | where process.parent.command_line == "run.bat"
            """);
        assertResultMap(
            result,
            List.of(columnInfo("process.parent.command_line", "keyword"), columnInfo("process.parent.command_line.text", "text")),
            List.of(matchesList().item("run.bat").item("run.bat"))
        );

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test | SORT Responses.process.pid"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(err, containsString("line 1:18: Unknown column [Responses.process.pid]"));

        e = expectThrows(ResponseException.class, () -> runEsql("""
            FROM test
            | SORT Responses.process.pid
            | WHERE Responses.process IS NULL
            """));
        err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(err, containsString("line 2:8: Unknown column [Responses.process.pid]"));
    }

    public void testOneNestedSubField_AndSameNameSupportedField_TwoIndices() throws IOException {
        assumeIndexResolverNestedFieldsNameClashFixed();
        ESRestTestCase.createIndex("test1", Settings.EMPTY, """
                  "properties": {
                    "Responses": {
                      "properties": {
                        "process": {
                          "type": "nested",
                          "properties": {
                            "pid": {
                              "type": "long"
                            }
                          }
                        }
                      }
                    }
                  }
            """);
        ESRestTestCase.createIndex("test2", Settings.EMPTY, """
                  "properties": {
                    "process": {
                      "properties": {
                        "parent": {
                          "properties": {
                            "command_line": {
                              "type": "wildcard",
                              "fields": {
                                "text": {
                                  "type": "text"
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
            """);
        index("test1", """
            {"Responses.process.pid": 123}""");
        index("test2", """
            {"process.parent.command_line":"run.bat"}""");

        Map<String, Object> result = runEsql("FROM test* | SORT process.parent.command_line ASC NULLS FIRST");
        assertResultMap(
            result,
            List.of(columnInfo("process.parent.command_line", "keyword"), columnInfo("process.parent.command_line.text", "text")),
            List.of(matchesList().item(null).item(null), matchesList().item("run.bat").item("run.bat"))
        );

        result = runEsql("""
            FROM test* | where process.parent.command_line == "run.bat"
            """);
        assertResultMap(
            result,
            List.of(columnInfo("process.parent.command_line", "keyword"), columnInfo("process.parent.command_line.text", "text")),
            List.of(matchesList().item("run.bat").item("run.bat"))
        );

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM test* | SORT Responses.process.pid"));
        String err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(err, containsString("line 1:19: Unknown column [Responses.process.pid]"));

        e = expectThrows(ResponseException.class, () -> runEsql("""
            FROM test*
            | SORT Responses.process.pid
            | WHERE Responses.process IS NULL
            """));
        err = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(err, containsString("line 2:8: Unknown column [Responses.process.pid]"));
    }

    public void testOneNestedField_AndSameNameSupportedField_TwoIndices() throws IOException {
        assumeIndexResolverNestedFieldsNameClashFixed();
        ESRestTestCase.createIndex("test1", Settings.EMPTY, """
            "properties": {
              "Responses": {
                "properties": {
                  "process": {
                    "type": "nested",
                    "properties": {
                      "pid": {
                        "type": "long"
                      }
                    }
                  }
                }
              },
              "process": {
                "properties": {
                  "parent": {
                    "properties": {
                      "command_line": {
                        "type": "wildcard",
                        "fields": {
                          "text": {
                            "type": "text"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """);
        ESRestTestCase.createIndex("test2", Settings.EMPTY, """
            "properties": {
              "Responses": {
                "properties": {
                  "process": {
                    "type": "integer",
                    "fields": {
                      "pid": {
                        "type": "long"
                      }
                    }
                  }
                }
              },
              "process": {
                "properties": {
                  "parent": {
                    "properties": {
                      "command_line": {
                        "type": "wildcard",
                        "fields": {
                          "text": {
                            "type": "text"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """);
        index("test1", """
            {"Responses.process.pid": 111,"process.parent.command_line":"run1.bat"}""");
        index("test2", """
            {"Responses.process": 222,"process.parent.command_line":"run2.bat"}""");

        Map<String, Object> result = runEsql("FROM test* | SORT process.parent.command_line");
        // If we're loading from _source we load the nested field.
        assertResultMap(
            result,
            List.of(
                columnInfo("Responses.process", "integer"),
                columnInfo("Responses.process.pid", "long"),
                columnInfo("process.parent.command_line", "keyword"),
                columnInfo("process.parent.command_line.text", "text")
            ),
            List.of(
                matchesList().item(null).item(pidMatcher()).item("run1.bat").item("run1.bat"),
                matchesList().item(222).item(222).item("run2.bat").item("run2.bat")
            )
        );

        result = runEsql("""
            FROM test* | where Responses.process.pid == 111
            """);
        assertResultMap(
            result,
            List.of(
                columnInfo("Responses.process", "integer"),
                columnInfo("Responses.process.pid", "long"),
                columnInfo("process.parent.command_line", "keyword"),
                columnInfo("process.parent.command_line.text", "text")
            ),
            List.of()
        );

        result = runEsql("FROM test* | SORT process.parent.command_line");
        assertResultMap(
            result,
            List.of(
                columnInfo("Responses.process", "integer"),
                columnInfo("Responses.process.pid", "long"),
                columnInfo("process.parent.command_line", "keyword"),
                columnInfo("process.parent.command_line.text", "text")
            ),
            List.of(
                matchesList().item(null).item(pidMatcher()).item("run1.bat").item("run1.bat"),
                matchesList().item(222).item(222).item("run2.bat").item("run2.bat")
            )
        );

        result = runEsql("""
            FROM test*
            | SORT process.parent.command_line
            | WHERE Responses.process IS NULL
            """);
        assertResultMap(
            result,
            List.of(
                columnInfo("Responses.process", "integer"),
                columnInfo("Responses.process.pid", "long"),
                columnInfo("process.parent.command_line", "keyword"),
                columnInfo("process.parent.command_line.text", "text")
            ),
            List.of(matchesList().item(null).item(pidMatcher()).item("run1.bat").item("run1.bat"))
        );
    }

    protected Matcher<Integer> pidMatcher() {
        // TODO these should all always return null because the parent is nested
        return preference == MappedFieldType.FieldExtractPreference.STORED ? equalTo(111) : nullValue(Integer.class);
    }

    private void assumeIndexResolverNestedFieldsNameClashFixed() throws IOException {
        // especially for BWC tests but also for regular tests
        var capsName = EsqlCapabilities.Cap.FIX_NESTED_FIELDS_NAME_CLASH_IN_INDEXRESOLVER.name().toLowerCase(Locale.ROOT);
        boolean requiredClusterCapability = clusterHasCapability("POST", "/_query", List.of(), List.of(capsName)).orElse(false);
        assumeTrue(
            "This test makes sense for versions that have the fix for https://github.com/elastic/elasticsearch/issues/117054",
            requiredClusterCapability
        );
    }

    private void assumeOriginalTypesReported() throws IOException {
        var capsName = EsqlCapabilities.Cap.REPORT_ORIGINAL_TYPES.name().toLowerCase(Locale.ROOT);
        boolean requiredClusterCapability = clusterHasCapability("POST", "/_query", List.of(), List.of(capsName)).orElse(false);
        assumeTrue("This test makes sense for versions that report original types", requiredClusterCapability);
    }

    private void assumeSuggestedCastReported() throws IOException {
        var capsName = EsqlCapabilities.Cap.SUGGESTED_CAST.name().toLowerCase(Locale.ROOT);
        boolean requiredClusterCapability = clusterHasCapability("POST", "/_query", List.of(), List.of(capsName)).orElse(false);
        assumeTrue("This test makes sense for versions that report suggested casts", requiredClusterCapability);
    }

    private CheckedConsumer<XContentBuilder, IOException> empNoInObject(String empNoType) {
        return index -> {
            index.startObject("properties");
            {
                index.startObject("foo");
                {
                    index.field("type", "object");
                    index.startObject("properties");
                    {
                        index.startObject("emp_no").field("type", empNoType).endObject();
                    }
                    index.endObject();
                }
                index.endObject();
            }
            index.endObject();
        };
    }

    private enum SourceMode {
        DEFAULT {
            @Override
            void sourceMapping(Settings.Builder builder) {}

            @Override
            boolean stored() {
                return true;
            }
        },
        STORED {
            @Override
            void sourceMapping(Settings.Builder builder) throws IOException {
                builder.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "stored");
            }

            @Override
            boolean stored() {
                return true;
            }
        },
        /* TODO add support to this test for disabling _source
        DISABLED {
            @Override
            void sourceMapping(XContentBuilder builder) throws IOException {
                builder.startObject(SourceFieldMapper.NAME).field("mode", "disabled").endObject();
            }

            @Override
            boolean stored() {
                return false;
            }
        },
         */
        SYNTHETIC {
            @Override
            void sourceMapping(Settings.Builder builder) throws IOException {
                builder.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic");
            }

            @Override
            boolean stored() {
                return false;
            }
        };

        abstract void sourceMapping(Settings.Builder builder) throws IOException;

        abstract boolean stored();
    }

    private boolean ignoredByIgnoreAbove(Integer ignoreAbove, int length) {
        return ignoreAbove != null && length > ignoreAbove;
    }

    private BigInteger randomUnsignedLong() {
        BigInteger big = BigInteger.valueOf(randomNonNegativeLong()).shiftLeft(1);
        return big.add(randomBoolean() ? BigInteger.ONE : BigInteger.ZERO);
    }

    private static String randomVersionString() {
        return randomVersionNumber() + (randomBoolean() ? "" : randomPrerelease());
    }

    private static String randomVersionNumber() {
        int numbers = between(1, 3);
        String v = Integer.toString(between(0, 100));
        for (int i = 1; i < numbers; i++) {
            v += "." + between(0, 100);
        }
        return v;
    }

    private static String randomPrerelease() {
        if (rarely()) {
            return randomFrom("alpha", "beta", "prerelease", "whatever");
        }
        return randomFrom("alpha", "beta", "") + randomVersionNumber();
    }

    private record StoreAndDocValues(Boolean store, Boolean docValues) {}

    private class Test {
        private final String type;
        private final Map<String, Test> subFields = new TreeMap<>();

        private SourceMode sourceMode;
        private String expectedType;
        private Function<SourceMode, Boolean> ignoreMalformed;
        private Function<SourceMode, StoreAndDocValues> storeAndDocValues = s -> new StoreAndDocValues(null, null);
        private Double scalingFactor;
        private Integer ignoreAbove;
        private Object value;
        private boolean createAlias;

        Test(String type) {
            this.type = type;
            // Default the expected return type to the field type.
            this.expectedType = type;
        }

        Test sourceMode(SourceMode sourceMode) {
            this.sourceMode = sourceMode;
            return this;
        }

        Test expectedType(String expectedType) {
            this.expectedType = expectedType;
            return this;
        }

        Test ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = s -> ignoreMalformed;
            return this;
        }

        /**
         * Enable {@code ignore_malformed} and disable synthetic _source because
         * most fields don't support ignore_malformed and synthetic _source.
         */
        Test forceIgnoreMalformed() {
            return this.sourceMode(randomValueOtherThan(SourceMode.SYNTHETIC, () -> randomFrom(SourceMode.values()))).ignoreMalformed(true);
        }

        Test randomIgnoreMalformedUnlessSynthetic() {
            this.ignoreMalformed = s -> s == SourceMode.SYNTHETIC ? false : randomBoolean();
            return this;
        }

        Test storeAndDocValues(Boolean store, Boolean docValues) {
            this.storeAndDocValues = s -> new StoreAndDocValues(store, docValues);
            return this;
        }

        Test randomStoreUnlessSynthetic() {
            this.storeAndDocValues = s -> new StoreAndDocValues(s == SourceMode.SYNTHETIC ? true : randomBoolean(), null);
            return this;
        }

        Test randomDocValuesAndStoreUnlessSynthetic() {
            this.storeAndDocValues = s -> {
                if (s == SourceMode.SYNTHETIC) {
                    boolean store = randomBoolean();
                    return new StoreAndDocValues(store, store == false || randomBoolean());
                }
                return new StoreAndDocValues(randomBoolean(), randomBoolean());
            };
            return this;
        }

        Test randomDocValuesUnlessSynthetic() {
            this.storeAndDocValues = s -> new StoreAndDocValues(null, s == SourceMode.SYNTHETIC || randomBoolean());
            return this;
        }

        Test scalingFactor(double scalingFactor) {
            this.scalingFactor = scalingFactor;
            return this;
        }

        Test ignoreAbove(Integer ignoreAbove) {
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        Test value(Object value) {
            this.value = value;
            return this;
        }

        Test createAlias() {
            this.createAlias = true;
            return this;
        }

        Test sub(String name, Test sub) {
            this.subFields.put(name, sub);
            return this;
        }

        Map<String, Object> roundTrip(Object value) throws IOException {
            String fieldName = type + "_field";
            createIndex("test", fieldName);
            if (randomBoolean()) {
                createIndex("test2", fieldName);
            }

            if (value == null) {
                logger.info("indexing empty doc");
                index("test", "{}");
            } else {
                logger.info("indexing {}::{}", value, value.getClass().getName());
                index("test", Strings.toString(JsonXContent.contentBuilder().startObject().field(fieldName, value).endObject()));
            }

            return fetchAll();
        }

        void test(Object value) throws IOException {
            test(value, value);
        }

        /**
         * Round trip the value through and index configured by the parameters
         * of this test and assert that it matches the {@code expectedValues}
         * which can be either the expected value or a subclass of {@link Matcher}.
         */
        void test(Object value, Object expectedValue) throws IOException {
            Map<String, Object> result = roundTrip(value);

            logger.info("expecting {}", expectedValue == null ? null : expectedValue + "::" + expectedValue.getClass().getName());

            List<Map<String, Object>> columns = new ArrayList<>();
            columns.add(columnInfo(type + "_field", expectedType));
            if (createAlias) {
                columns.add(columnInfo("a.b.c." + type + "_field_alias", expectedType));
                columns.add(columnInfo(type + "_field_alias", expectedType));
            }
            Collections.sort(columns, Comparator.comparing(m -> (String) m.get("name")));

            ListMatcher values = matchesList();
            values = values.item(expectedValue);
            if (createAlias) {
                values = values.item(expectedValue);
                values = values.item(expectedValue);
            }

            assertResultMap(result, columns, List.of(values));
        }

        void createIndex(String name, String fieldName) throws IOException {
            if (sourceMode == null) {
                sourceMode(randomFrom(SourceMode.values()));
            }
            logger.info("source_mode: {}", sourceMode);

            Settings.Builder settings = Settings.builder();
            sourceMode.sourceMapping(settings);

            FieldExtractorTestCase.createIndex(name, index -> {
                index.startObject("properties");
                {
                    index.startObject(fieldName);
                    fieldMapping(index);
                    index.endObject();

                    if (createAlias) {
                        // create two aliases - one within a hierarchy, the other just a simple field w/o hierarchy
                        index.startObject(fieldName + "_alias");
                        {
                            index.field("type", "alias");
                            index.field("path", fieldName);
                        }
                        index.endObject();
                        index.startObject("a.b.c." + fieldName + "_alias");
                        {
                            index.field("type", "alias");
                            index.field("path", fieldName);
                        }
                        index.endObject();
                    }
                }
                index.endObject();
            });
        }

        private void fieldMapping(XContentBuilder builder) throws IOException {
            builder.field("type", type);
            if (ignoreMalformed != null) {
                boolean v = ignoreMalformed.apply(sourceMode);
                builder.field("ignore_malformed", v);
                ignoreMalformed = m -> v;
            }
            StoreAndDocValues sd = storeAndDocValues.apply(sourceMode);
            storeAndDocValues = m -> sd;
            if (sd.docValues != null) {
                builder.field("doc_values", sd.docValues);
            }
            if (sd.store != null) {
                builder.field("store", sd.store);
            }
            if (scalingFactor != null) {
                builder.field("scaling_factor", scalingFactor);
            }
            if (ignoreAbove != null) {
                builder.field("ignore_above", ignoreAbove);
            }
            if (value != null) {
                builder.field("value", value);
            }

            if (subFields.isEmpty() == false) {
                builder.startObject("fields");
                for (Map.Entry<String, Test> sub : subFields.entrySet()) {
                    builder.startObject(sub.getKey());
                    if (sub.getValue().sourceMode == null) {
                        sub.getValue().sourceMode = sourceMode;
                    } else if (sub.getValue().sourceMode != sourceMode) {
                        throw new IllegalStateException("source_mode can't be configured on sub-fields");
                    }
                    sub.getValue().fieldMapping(builder);
                    builder.endObject();
                }
                builder.endObject();
            }
        }

        private Map<String, Object> fetchAll() throws IOException {
            return runEsql("FROM test* | LIMIT 10");
        }
    }

    private static Map<String, Object> columnInfo(String name, String type) {
        return Map.of("name", name, "type", type);
    }

    private static Map<String, Object> unsupportedColumnInfo(String name, String... originalTypes) {
        DataType suggested = DataType.suggestedCast(
            List.of(originalTypes).stream().map(DataType::fromTypeName).filter(Objects::nonNull).collect(Collectors.toSet())
        );
        if (suggested == null) {
            return Map.of("name", name, "type", "unsupported", "original_types", List.of(originalTypes));
        } else {
            return Map.ofEntries(
                Map.entry("name", name),
                Map.entry("type", "unsupported"),
                Map.entry("original_types", List.of(originalTypes)),
                Map.entry("suggested_cast", suggested.typeName())
            );
        }
    }

    private static void index(String name, String... docs) throws IOException {
        Request request = new Request("POST", "/" + name + "/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (String doc : docs) {
            bulk.append(String.format(Locale.ROOT, """
                {"index":{}}
                %s
                """, doc));
        }
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        Map<String, Object> result = entityToMap(response.getEntity(), XContentType.JSON);
        assertMap(result, matchesMap().extraOk().entry("errors", false));
    }

    private static void createIndex(String name, CheckedConsumer<XContentBuilder, IOException> mapping) throws IOException {
        XContentBuilder index = JsonXContent.contentBuilder().prettyPrint().startObject();
        mapping.accept(index);
        index.endObject();
        String configStr = Strings.toString(index);
        logger.info("index: {} {}", name, configStr);
        ESRestTestCase.createIndex(name, Settings.EMPTY, configStr);
    }

    private static void createIndex(String name, Settings setting, CheckedConsumer<XContentBuilder, IOException> mapping)
        throws IOException {
        XContentBuilder index = JsonXContent.contentBuilder().prettyPrint().startObject();
        mapping.accept(index);
        index.endObject();
        String configStr = Strings.toString(index);
        logger.info("index: {} {}", name, configStr);
        ESRestTestCase.createIndex(name, setting, configStr);
    }

    /**
     * Yaml adds newlines and some indentation which we don't want to match.
     */
    private String deyaml(String err) {
        return err.replaceAll("\\\\\n\s+\\\\", "");
    }

    private Map<String, Object> runEsql(String query) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder request = new RestEsqlTestCase.RequestObjectBuilder().query(query);
        if (preference != null) {
            canUsePragmasOk();
            request = request.pragmas(
                Settings.builder().put(QueryPragmas.FIELD_EXTRACT_PREFERENCE.getKey(), preference.toString()).build()
            );
            request.pragmasOk();
        }
        return runEsqlSync(request, new AssertWarnings.NoWarnings(), profileLogger);
    }

    protected abstract void canUsePragmasOk();
}
