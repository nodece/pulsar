/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.tests.integration.cli.tenant;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.Test;

public class TenantTest extends PulsarTestSuite {
    @Test
    public void testListTenantCmd() throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("tenants list");
        result.assertZeroExitCode();
        assertTrue(result.getStdout().contains("public"));
    }

    @Test
    public void testGetTenantCmd() throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("tenants get public");
        result.assertZeroExitCode();
    }

    @Test
    public void testGetNonExistTenantCmd() throws Exception {
        String tenantName = randomName();
        String getCommand = String.format("tenants get %s", tenantName);
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(getCommand);
        result.assertNonZeroExitCode();
        assertTrue(result.getStdout().contains("Tenant does not exist"));
    }

    @Test
    public void testCreateTenantCmd() throws Exception {
        String tenantName = randomName();
        String createCommand = String.format("tenants create %s", tenantName);
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(createCommand);
        result.assertZeroExitCode();

        String getCommand = String.format("tenants get %s", tenantName);
        result = pulsarCluster.runAdminCommandOnAnyBroker(getCommand);
        result.assertZeroExitCode();
        TenantInfo tenantInfo = gson.fromJson(result.getStdout(), TenantInfo.class);
        assertNotNull(tenantInfo);
        assertTrue(tenantInfo.getAdminRoles().isEmpty());
        assertFalse(tenantInfo.getAllowedClusters().isEmpty());

        String deleteCommand = String.format("tenants delete %s", tenantName);
        result = pulsarCluster.runAdminCommandOnAnyBroker(deleteCommand);
        result.assertZeroExitCode();
    }

    @Test
    public void testCreateTenantCmdWithAdminRolesAndAllowClustersFlags() throws Exception {
        String tenantName = randomName();
        List<String> adminRoles = Arrays.asList("role1", "role2");
        List<String> allowedClusters = Collections.singletonList(pulsarCluster.getClusterName());

        String createCommand = String.format("tenants create %s --admin-roles %s --allowed-clusters %s", tenantName,
                String.join(",", adminRoles), String.join(",", allowedClusters));
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(createCommand);
        result.assertZeroExitCode();

        String getCommand = String.format("tenants get %s", tenantName);
        result = pulsarCluster.runAdminCommandOnAnyBroker(getCommand);
        result.assertZeroExitCode();
        TenantInfo tenantInfo = gson.fromJson(result.getStdout(), TenantInfo.class);
        assertNotNull(tenantInfo);
        assertEquals(tenantInfo.getAdminRoles(), adminRoles);
        assertEquals(tenantInfo.getAllowedClusters(), allowedClusters);

        String deleteCommand = String.format("tenants delete %s", tenantName);
        result = pulsarCluster.runAdminCommandOnAnyBroker(deleteCommand);
        result.assertZeroExitCode();
    }

    @Test
    public void testCreateExistTenantCmd() throws Exception {
        String tenantName = randomName();
        String createCommand = String.format("tenants create %s", tenantName);
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(createCommand);
        result.assertNonZeroExitCode();
        assertTrue(result.getStdout().contains("Tenant already exist"));
    }

    @Test
    public void testUpdateTenantCmdWithAdminRolesAndAllowedClustersFlags() throws Exception {
        String tenantName = randomName();
        List<String> adminRoles = Arrays.asList("role1", "role2");
        String createCommand =
                String.format("tenants create %s --admin-roles %s", tenantName, String.join(",", adminRoles));
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(createCommand);
        result.assertZeroExitCode();

        String getCommand = String.format("tenants get %s", tenantName);
        result = pulsarCluster.runAdminCommandOnAnyBroker(getCommand);
        result.assertZeroExitCode();
        TenantInfo tenantInfo = gson.fromJson(result.getStdout(), TenantInfo.class);
        assertNotNull(tenantInfo);
        assertEquals(tenantInfo.getAdminRoles(), adminRoles);
        assertFalse(tenantInfo.getAllowedClusters().isEmpty());

        adminRoles = Arrays.asList("role3", "role4");
        List<String> allowedClusters = Collections.singletonList(pulsarCluster.getClusterName());
        String updateCommand = String.format("tenants update %s --admin-roles %s --allowed-clusters %s", tenantName,
                String.join(",", adminRoles), String.join(",", allowedClusters));
        result = pulsarCluster.runAdminCommandOnAnyBroker(updateCommand);
        result.assertZeroExitCode();

        result = pulsarCluster.runAdminCommandOnAnyBroker(getCommand);
        result.assertZeroExitCode();
        tenantInfo = gson.fromJson(result.getStdout(), TenantInfo.class);
        assertNotNull(tenantInfo);
        assertEquals(tenantInfo.getAdminRoles(), adminRoles);
        assertEquals(tenantInfo.getAllowedClusters(), allowedClusters);

        String deleteCommand = String.format("tenants delete %s", tenantName);
        result = pulsarCluster.runAdminCommandOnAnyBroker(deleteCommand);
        result.assertZeroExitCode();
    }
}
