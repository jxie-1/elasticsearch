/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.xpack.writeloadforecaster.WriteLoadForecasterPlugin.OVERRIDE_WRITE_LOAD_FORECAST_SETTING;

class LicensedWriteLoadForecaster extends AbstractLicenseCheckingWriteLoadForecaster {

    private static final Logger logger = LogManager.getLogger(LicensedWriteLoadForecaster.class);

    public static final Setting<TimeValue> MAX_INDEX_AGE_SETTING = Setting.timeSetting(
        "write_load_forecaster.max_index_age",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueHours(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How the write load of the recent indices of a data stream is combined into the forecast for the next write index.
     */
    public enum Aggregation {
        /**
         * The average write load of the recent indices, each weighted by how long it lived. Long-lived, lightly loaded indices
         * outweigh short-lived, heavily loaded ones, so a stream that is much busier during part of the day is forecast well below
         * its busy load.
         */
        WEIGHTED_AVERAGE,
        /**
         * The highest peak write load seen in any of the recent indices. A busy period is not diluted by the quiet ones, at the
         * price of over-forecasting after an unusually busy index.
         */
        MAX_PEAK
    }

    public static final Setting<Aggregation> AGGREGATION_SETTING = Setting.enumSetting(
        Aggregation.class,
        "write_load_forecaster.aggregation",
        Aggregation.WEIGHTED_AVERAGE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ThreadPool threadPool;
    private volatile TimeValue maxIndexAge;
    private volatile Aggregation aggregation;

    // hasValidLicense is a protected field in AbstractLicenseCheckingWriteLoadForecaster

    LicensedWriteLoadForecaster(
        BooleanSupplier hasValidLicenseSupplier,
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        this(hasValidLicenseSupplier, threadPool, MAX_INDEX_AGE_SETTING.get(settings), AGGREGATION_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(MAX_INDEX_AGE_SETTING, this::setMaxIndexAgeSetting);
        clusterSettings.addSettingsUpdateConsumer(AGGREGATION_SETTING, this::setAggregationSetting);
    }

    // exposed for tests only
    LicensedWriteLoadForecaster(BooleanSupplier hasValidLicenseSupplier, ThreadPool threadPool, TimeValue maxIndexAge) {
        this(hasValidLicenseSupplier, threadPool, maxIndexAge, Aggregation.WEIGHTED_AVERAGE);
    }

    // exposed for tests only
    LicensedWriteLoadForecaster(
        BooleanSupplier hasValidLicenseSupplier,
        ThreadPool threadPool,
        TimeValue maxIndexAge,
        Aggregation aggregation
    ) {
        super(hasValidLicenseSupplier);
        this.threadPool = threadPool;
        this.maxIndexAge = maxIndexAge;
        this.aggregation = aggregation;
    }

    private void setMaxIndexAgeSetting(TimeValue updatedMaxIndexAge) {
        this.maxIndexAge = updatedMaxIndexAge;
    }

    private void setAggregationSetting(Aggregation updatedAggregation) {
        this.aggregation = updatedAggregation;
    }

    @Override
    public ProjectMetadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, ProjectMetadata.Builder metadata) {
        if (hasValidLicense == false) {
            return metadata;
        }

        final DataStream dataStream = metadata.dataStream(dataStreamName);

        if (dataStream == null) {
            return metadata;
        }

        clearPreviousForecast(dataStream, metadata);

        final List<IndexWriteLoad> indicesWriteLoadWithinMaxAgeRange = DataStream.getIndicesWithinMaxAgeRange(
            dataStream,
            metadata::getSafe,
            maxIndexAge,
            threadPool::absoluteTimeInMillis
        )
            .stream()
            .filter(index -> index.equals(dataStream.getWriteIndex()) == false)
            .map(metadata::getSafe)
            .map(IndexMetadata::getStats)
            .filter(Objects::nonNull)
            .map(IndexMetadataStats::writeLoad)
            .filter(Objects::nonNull)
            .toList();

        OptionalDouble forecastIndexWriteLoad = forecastIndexWriteLoad(indicesWriteLoadWithinMaxAgeRange, aggregation);

        if (forecastIndexWriteLoad.isEmpty()) {
            return metadata;
        }

        final IndexMetadata writeIndex = metadata.getSafe(dataStream.getWriteIndex());
        metadata.put(
            IndexMetadata.builder(writeIndex)
                .indexWriteLoadForecast(forecastIndexWriteLoad.getAsDouble() / writeIndex.getNumberOfShards())
                .build(),
            false
        );

        return metadata;
    }

    private static void clearPreviousForecast(DataStream dataStream, ProjectMetadata.Builder metadata) {
        if (dataStream.getIndices().size() > 1) {
            final Index previousWriteIndex = dataStream.getIndices().get(dataStream.getIndices().size() - 2);
            final IndexMetadata previousWriteIndexMetadata = metadata.getSafe(previousWriteIndex);
            final IndexMetadata.Builder previousWriteIndexMetadataBuilder = IndexMetadata.builder(previousWriteIndexMetadata)
                .indexWriteLoadForecast(null);
            if (previousWriteIndexMetadata.getSettings().hasValue(OVERRIDE_WRITE_LOAD_FORECAST_SETTING.getKey())) {
                Settings.Builder previousWriteIndexSettings = Settings.builder()
                    .put(previousWriteIndexMetadata.getSettings())
                    .remove(OVERRIDE_WRITE_LOAD_FORECAST_SETTING.getKey());
                previousWriteIndexMetadataBuilder.settings(previousWriteIndexSettings);
                previousWriteIndexMetadataBuilder.settingsVersion(previousWriteIndexMetadata.getSettingsVersion() + 1);
            }
            metadata.put(previousWriteIndexMetadataBuilder.build(), false);
        }
    }

    /**
     * This calculates the weighted average total write-load for all recent indices.
     *
     * @param indicesWriteLoadWithinMaxAgeRange The indices considered "recent"
     * @return The weighted average total write-load. To get the per-shard write load, this number must be divided by the number of shards
     */
    // Visible for testing
    static OptionalDouble forecastIndexWriteLoad(List<IndexWriteLoad> indicesWriteLoadWithinMaxAgeRange) {
        return forecastIndexWriteLoad(indicesWriteLoadWithinMaxAgeRange, Aggregation.WEIGHTED_AVERAGE);
    }

    /**
     * This calculates the total write-load to forecast for the next index, from the write load of all recent indices.
     *
     * @param indicesWriteLoadWithinMaxAgeRange The indices considered "recent"
     * @param aggregation How the recent indices are combined
     * @return The total write-load. To get the per-shard write load, this number must be divided by the number of shards
     */
    // Visible for testing
    static OptionalDouble forecastIndexWriteLoad(List<IndexWriteLoad> indicesWriteLoadWithinMaxAgeRange, Aggregation aggregation) {
        return switch (aggregation) {
            case WEIGHTED_AVERAGE -> weightedAverageIndexWriteLoad(indicesWriteLoadWithinMaxAgeRange);
            case MAX_PEAK -> maxPeakIndexWriteLoad(indicesWriteLoadWithinMaxAgeRange);
        };
    }

    /**
     * The highest total peak write-load of any of the indices. The peak of an index is the sum of the peaks of its shards: shards with no
     * peak recorded are assumed to have the average peak of the others, as for the weighted average, and indices without any are ignored.
     */
    private static OptionalDouble maxPeakIndexWriteLoad(List<IndexWriteLoad> indicesWriteLoadWithinMaxAgeRange) {
        double maxIndexPeakWriteLoad = 0;
        boolean anyPeakWriteLoad = false;
        for (IndexWriteLoad writeLoad : indicesWriteLoadWithinMaxAgeRange) {
            double totalShardPeakWriteLoad = 0;
            int shardsWithPeakWriteLoad = 0;
            for (int shardId = 0; shardId < writeLoad.numberOfShards(); shardId++) {
                final OptionalDouble peakWriteLoadForShard = writeLoad.getPeakWriteLoadForShard(shardId);
                if (peakWriteLoadForShard.isPresent()) {
                    totalShardPeakWriteLoad += peakWriteLoadForShard.getAsDouble();
                    shardsWithPeakWriteLoad++;
                }
            }
            if (shardsWithPeakWriteLoad == 0) {
                continue;
            }
            final double totalIndexPeakWriteLoad = totalShardPeakWriteLoad / shardsWithPeakWriteLoad * writeLoad.numberOfShards();
            assert Double.isFinite(totalIndexPeakWriteLoad) : "Invalid total index peak write load: " + totalIndexPeakWriteLoad;
            maxIndexPeakWriteLoad = Math.max(maxIndexPeakWriteLoad, totalIndexPeakWriteLoad);
            anyPeakWriteLoad = true;
        }
        return anyPeakWriteLoad ? OptionalDouble.of(maxIndexPeakWriteLoad) : OptionalDouble.empty();
    }

    private static OptionalDouble weightedAverageIndexWriteLoad(List<IndexWriteLoad> indicesWriteLoadWithinMaxAgeRange) {
        double allIndicesWriteLoad = 0;
        long allIndicesUptime = 0;
        for (IndexWriteLoad writeLoad : indicesWriteLoadWithinMaxAgeRange) {
            double totalShardWriteLoad = 0;
            long totalShardUptimeInMillis = 0;
            long maxShardUptimeInMillis = 0;
            for (int shardId = 0; shardId < writeLoad.numberOfShards(); shardId++) {
                final OptionalDouble writeLoadForShard = writeLoad.getWriteLoadForShard(shardId);
                final OptionalLong uptimeInMillisForShard = writeLoad.getUptimeInMillisForShard(shardId);
                if (writeLoadForShard.isPresent()) {
                    assert uptimeInMillisForShard.isPresent();
                    double shardWriteLoad = writeLoadForShard.getAsDouble();
                    long shardUptimeInMillis = uptimeInMillisForShard.getAsLong();
                    totalShardWriteLoad += shardWriteLoad * shardUptimeInMillis;
                    totalShardUptimeInMillis += shardUptimeInMillis;
                    maxShardUptimeInMillis = Math.max(maxShardUptimeInMillis, shardUptimeInMillis);
                }
            }
            // An index only contributes to the weighted average proportionally to its uptime
            if (totalShardUptimeInMillis == 0) {
                continue;
            }
            double weightedAverageShardWriteLoad = totalShardWriteLoad / totalShardUptimeInMillis;
            double totalIndexWriteLoad = weightedAverageShardWriteLoad * writeLoad.numberOfShards();
            // We need to weight the contribution from each index somehow, but we only know
            // the write-load from the final allocation of each shard at rollover time. It's
            // possible the index is much older than any of those shards, but we don't have
            // any write-load data beyond their lifetime.
            // To avoid making assumptions about periods for which we have no data, we'll weight
            // each index's contribution to the forecast by the maximum shard uptime observed in
            // that index. It should be safe to extrapolate our weighted average out to the
            // maximum uptime observed, based on the assumption that write-load is roughly
            // evenly distributed across shards of a datastream index.
            assert Double.isFinite(totalIndexWriteLoad) : "Invalid total index write load: " + totalIndexWriteLoad;
            assert maxShardUptimeInMillis > 0 : "Invalid max shard uptime in millis: " + maxShardUptimeInMillis;
            allIndicesWriteLoad += totalIndexWriteLoad * maxShardUptimeInMillis;
            allIndicesUptime += maxShardUptimeInMillis;
        }

        return allIndicesUptime == 0 ? OptionalDouble.empty() : OptionalDouble.of(allIndicesWriteLoad / allIndicesUptime);
    }

    @Override
    @SuppressForbidden(reason = "This is the only place where IndexMetadata#getForecastedWriteLoad is allowed to be used")
    public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
        if (hasValidLicense == false) {
            return OptionalDouble.empty();
        }

        if (OVERRIDE_WRITE_LOAD_FORECAST_SETTING.exists(indexMetadata.getSettings())) {
            Double overrideWriteLoadForecast = OVERRIDE_WRITE_LOAD_FORECAST_SETTING.get(indexMetadata.getSettings());
            return OptionalDouble.of(overrideWriteLoadForecast);
        }

        return indexMetadata.getForecastedWriteLoad();
    }
}
