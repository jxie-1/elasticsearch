/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.cluster.coordination.votingonly;

import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class VotingOnlyNodePluginTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateVotingOnlyNodePlugin.class, RepositoryVerifyAccessPlugin.class);
    }

    public void testRequireVotingOnlyNodeToBeMasterEligible() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> internalCluster().startNode(Settings.builder().put(onlyRole(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)).build())
        );
        assertThat(e.getMessage(), containsString("voting-only node must be master-eligible"));
    }

    public void testVotingOnlyNodeStats() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNodes(2);
        internalCluster().startNode(addRoles(Set.of(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)));
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().getLastCommittedConfiguration().getNodeIds(),
                hasSize(3)
            )
        );
        assertThat(
            clusterAdmin().prepareClusterStats()
                .get()
                .getNodesStats()
                .getCounts()
                .getRoles()
                .get(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE.roleName())
                .intValue(),
            equalTo(1)
        );
        assertThat(clusterAdmin().prepareNodesStats("voting_only:true").get().getNodes(), hasSize(1));
        assertThat(clusterAdmin().prepareNodesStats("master:true", "voting_only:false").get().getNodes(), hasSize(2));
    }

    public void testPreferFullMasterOverVotingOnlyNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNodes(2);
        internalCluster().startNode(addRoles(Set.of(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)));
        final int numDataNodes = randomInt(2);
        internalCluster().startDataOnlyNodes(numDataNodes);
        internalCluster().validateClusterFormed();

        awaitClusterState(
            state -> state.getLastCommittedConfiguration().getNodeIds().size() == 3 && state.nodes().size() == 3 + numDataNodes
        );
        final String originalMaster = internalCluster().getMasterName();

        internalCluster().stopCurrentMasterNode();
        awaitMasterNode();
        assertNotEquals(originalMaster, internalCluster().getMasterName());
        assertThat(
            VotingOnlyNodePlugin.isVotingOnlyNode(
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getMasterNode()
            ),
            equalTo(false)
        );
    }

    public void testBootstrapOnlyVotingOnlyNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNodes(addRoles(Set.of(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)), Settings.EMPTY, Settings.EMPTY);
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().getLastCommittedConfiguration().getNodeIds().size(),
                equalTo(3)
            )
        );
        awaitMasterNode();
        assertThat(
            VotingOnlyNodePlugin.isVotingOnlyNode(
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getMasterNode()
            ),
            equalTo(false)
        );
    }

    public void testBootstrapOnlySingleVotingOnlyNode() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode(
            Settings.builder()
                .put(addRoles(Set.of(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)))
                .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s")
                .build()
        );
        internalCluster().startNode();
        ensureStableCluster(2);
        awaitMasterNode();
        assertThat(
            VotingOnlyNodePlugin.isVotingOnlyNode(
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getMasterNode()
            ),
            equalTo(false)
        );
    }

    public void testVotingOnlyNodesCannotBeMasterWithoutFullMasterNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        internalCluster().startNodes(2, addRoles(Set.of(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)));
        final int numDataNodes = randomInt(2);
        internalCluster().startDataOnlyNodes(numDataNodes);
        internalCluster().validateClusterFormed();

        awaitClusterState(
            state -> state.getLastCommittedConfiguration().getNodeIds().size() == 3 && state.nodes().size() == 3 + numDataNodes
        );
        final String oldMasterId = internalCluster().getMasterName();

        internalCluster().stopCurrentMasterNode();
        awaitMasterNotFound();

        // start a fresh full master node, which will be brought into the cluster as master by the voting-only nodes
        final String newMaster = internalCluster().startNode();
        assertEquals(newMaster, internalCluster().getMasterName());
        final String newMasterId = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getMasterNodeId();
        assertNotEquals(oldMasterId, newMasterId);
    }

    public void testBasicSnapshotRestoreWorkFlow() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNodes(2);
        // dedicated voting-only master node
        final String dedicatedVotingOnlyNode = internalCluster().startNode(
            onlyRoles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE))
        );
        // voting-only master node that also has data
        Settings dataContainingVotingOnlyNodeSettings = addRoles(Set.of(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE));
        if (randomBoolean()) {
            dataContainingVotingOnlyNodeSettings = removeRoles(dataContainingVotingOnlyNodeSettings, Set.of(DiscoveryNodeRole.DATA_ROLE));
        }
        final String nonDedicatedVotingOnlyNode = internalCluster().startNode(dataContainingVotingOnlyNodeSettings);

        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("verifyaccess-fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", randomBoolean()))
        );
        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        VerifyRepositoryResponse verifyResponse = clusterAdmin().prepareVerifyRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "test-repo"
        ).get();
        // only the da
        assertEquals(3, verifyResponse.getNodes().size());
        assertTrue(verifyResponse.getNodes().stream().noneMatch(nw -> nw.getName().equals(dedicatedVotingOnlyNode)));
        assertTrue(verifyResponse.getNodes().stream().anyMatch(nw -> nw.getName().equals(nonDedicatedVotingOnlyNode)));

        final String[] indicesToSnapshot = { "test-idx-*", "-test-idx-3" };

        logger.info("--> snapshot");
        Client client = client();
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices(indicesToSnapshot)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            Matchers.equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        List<SnapshotInfo> snapshotInfos = client.admin()
            .cluster()
            .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "test-repo")
            .setSnapshots(randomFrom("test-snap", "_all", "*", "*-snap", "test*"))
            .get()
            .getSnapshots();
        assertThat(snapshotInfos.size(), Matchers.equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), Matchers.equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), Matchers.equalTo(IndexVersion.current()));

        logger.info("--> close indices");
        client.admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
    }

    public static class RepositoryVerifyAccessPlugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Collections.singletonMap(
                "verifyaccess-fs",
                (projectId, metadata) -> new AccessVerifyingRepo(
                    projectId,
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings
                )
            );
        }

        private static class AccessVerifyingRepo extends FsRepository {

            private final ClusterService clusterService;

            private AccessVerifyingRepo(
                ProjectId projectId,
                RepositoryMetadata metadata,
                Environment environment,
                NamedXContentRegistry namedXContentRegistry,
                ClusterService clusterService,
                BigArrays bigArrays,
                RecoverySettings recoverySettings
            ) {
                super(projectId, metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
                this.clusterService = clusterService;
            }

            @Override
            protected BlobStore createBlobStore() throws Exception {
                final DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
                if (localNode.getRoles().contains(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)) {
                    assertTrue(localNode.canContainData());
                }
                return super.createBlobStore();
            }
        }
    }
}
