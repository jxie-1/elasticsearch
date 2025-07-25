/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.apache.lucene.tests.mockfile.FilterFileSystem;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TestPathLookup implements PathLookup {
    final Map<BaseDir, Collection<Path>> baseDirPaths;

    public TestPathLookup(Map<BaseDir, Collection<Path>> baseDirPaths) {
        this.baseDirPaths = baseDirPaths;
    }

    @Override
    public Path pidFile() {
        return null;
    }

    @Override
    public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
        return baseDirPaths.getOrDefault(baseDir, List.of()).stream();
    }

    @Override
    public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
        return Stream.empty();
    }

    @Override
    public boolean isPathOnDefaultFilesystem(Path path) {
        var fileSystem = path.getFileSystem();
        if (fileSystem.getClass() != DEFAULT_FILESYSTEM_CLASS) {
            while (fileSystem instanceof FilterFileSystem ffs) {
                fileSystem = ffs.getDelegate();
            }
        }
        return fileSystem.getClass() == DEFAULT_FILESYSTEM_CLASS;
    }
}
