/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package globalranktable

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
)

// createTestHyperJob creates a mock HyperJob for testing.
func createTestHyperJob(name, namespace string) *trainingv1alpha1.HyperJob {
	return &trainingv1alpha1.HyperJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       "test-uid",
		},
	}
}

// createTestVCJob creates a mock VCJob with specified environment variables for testing.
func createTestVCJob(name, namespace string, envs []corev1.EnvVar) *batchv1alpha1.Job {
	return &batchv1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: batchv1alpha1.JobSpec{
			Tasks: []batchv1alpha1.TaskSpec{
				{
					Name:     "worker",
					Replicas: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "test-image",
									Env:   envs,
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestOnHyperJobAdd(t *testing.T) {
	tests := []struct {
		name                string
		pluginArgs          []string
		expectedConfigmaps  []string
		expectCreationError bool
	}{
		{
			name:                "Success with default args",
			pluginArgs:          []string{},
			expectedConfigmaps:  defaultConfigmapsNeedToCreate,
			expectCreationError: false,
		},
		{
			name:                "Success with custom configmaps",
			pluginArgs:          []string{"--configmaps-to-create=cm1,cm2"},
			expectedConfigmaps:  []string{"cm1", "cm2"},
			expectCreationError: false,
		},
		{
			name:                "Failure with invalid args",
			pluginArgs:          []string{"--ai-accelerate-policy=invalid"},
			expectedConfigmaps:  nil,
			expectCreationError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hj := createTestHyperJob("test-hj", "test-ns")
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = trainingv1alpha1.AddToScheme(scheme)
			_ = batchv1alpha1.AddToScheme(scheme)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			plugin, err := NewPlugin(fakeClient, tt.pluginArgs)

			if tt.expectCreationError {
				assert.Error(t, err)
				assert.Nil(t, plugin)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, plugin)
			grpPlugin := plugin.(*GlobalRanktablePlugin)

			err = grpPlugin.OnHyperJobAdd(context.Background(), hj)
			assert.NoError(t, err, "OnHyperJobAdd should not return an error")

			for _, cmResourceName := range tt.expectedConfigmaps {
				createdCM := &corev1.ConfigMap{}
				expectedCMName := fmt.Sprintf("%s-%s", hj.Name, cmResourceName)
				errGet := grpPlugin.client.Get(context.Background(), types.NamespacedName{Name: expectedCMName, Namespace: hj.Namespace}, createdCM)

				assert.NoError(t, errGet, "Expected ConfigMap %s to be created", expectedCMName)
				if errGet == nil {
					assert.Nil(t, createdCM.Data, "Expected ConfigMap %s to be empty", expectedCMName)
					assert.Len(t, createdCM.OwnerReferences, 1, "Expected one owner reference for %s", expectedCMName)

					ownerRef := createdCM.OwnerReferences[0]
					assert.Equal(t, hj.Name, ownerRef.Name)
					assert.Equal(t, hj.UID, ownerRef.UID)
					assert.True(t, *ownerRef.Controller)
				}
			}
		})
	}
}

func TestOnJobCreate(t *testing.T) {
	tests := []struct {
		name                    string
		pluginArgs              []string
		vcJobEnvs               []corev1.EnvVar
		expectedMounts          map[string]string
		expectedVCJobPluginArgs []string
		preExistingVCJobPlugin  bool
	}{
		{
			name:       "Default args and mount paths",
			pluginArgs: []string{},
			vcJobEnvs:  []corev1.EnvVar{},
			expectedMounts: map[string]string{
				ConfigmapGlobalRankTable:    fmt.Sprintf("%s/%s", DefaultMountPrefix, ConfigmapGlobalRankTable),
				ConfigmapGlobalNetWorkLinks: fmt.Sprintf("%s/%s", DefaultMountPrefix, ConfigmapGlobalNetWorkLinks),
			},
			expectedVCJobPluginArgs: []string{
				fmt.Sprintf("--rank-table-version=%s", DefaultGlobalRankTableVersion),
			},
		},
		{
			name:       "Custom mount paths from env",
			pluginArgs: []string{},
			vcJobEnvs: []corev1.EnvVar{
				{Name: GlobalRankTableFilePath, Value: "/custom/path/global-ranktable"},
				{Name: GlobalNetworkLinksFilePath, Value: "/custom/path/global-network-links"},
			},
			expectedMounts: map[string]string{
				ConfigmapGlobalRankTable:    "/custom/path/global-ranktable",
				ConfigmapGlobalNetWorkLinks: "/custom/path/global-network-links",
			},
			expectedVCJobPluginArgs: []string{
				fmt.Sprintf("--rank-table-version=%s", DefaultGlobalRankTableVersion),
			},
		},
		{
			name:       "PSM policy enabled",
			pluginArgs: []string{"--ai-accelerate-policy=psm", "--rank-table-version=1.4"},
			vcJobEnvs:  []corev1.EnvVar{},
			expectedMounts: map[string]string{
				ConfigmapGlobalRankTable:    fmt.Sprintf("%s/%s", DefaultMountPrefix, ConfigmapGlobalRankTable),
				ConfigmapGlobalNetWorkLinks: fmt.Sprintf("%s/%s", DefaultMountPrefix, ConfigmapGlobalNetWorkLinks),
			},
			expectedVCJobPluginArgs: []string{
				"--rank-table-version=1.4",
				"--rank-table-tor-enable=true",
			},
		},
		{
			name:       "VCJob already has plugin, args should not be overwritten",
			pluginArgs: []string{"--ai-accelerate-policy=psm"},
			vcJobEnvs:  []corev1.EnvVar{},
			expectedMounts: map[string]string{
				ConfigmapGlobalRankTable:    fmt.Sprintf("%s/%s", DefaultMountPrefix, ConfigmapGlobalRankTable),
				ConfigmapGlobalNetWorkLinks: fmt.Sprintf("%s/%s", DefaultMountPrefix, ConfigmapGlobalNetWorkLinks),
			},
			expectedVCJobPluginArgs: []string{"--rank-table-version=1.2"},
			preExistingVCJobPlugin:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup plugin
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = trainingv1alpha1.AddToScheme(scheme)
			_ = batchv1alpha1.AddToScheme(scheme)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			plugin, err := NewPlugin(fakeClient, tt.pluginArgs)
			assert.NoError(t, err)
			grpPlugin := plugin.(*GlobalRanktablePlugin)

			hj := createTestHyperJob("test-hj", "test-ns")
			vcJob := createTestVCJob("test-vcjob", "test-ns", tt.vcJobEnvs)

			if tt.preExistingVCJobPlugin {
				vcJob.Spec.Plugins = map[string][]string{
					VCJobPluginName: tt.expectedVCJobPluginArgs,
				}
			}

			err = grpPlugin.OnJobCreate(context.Background(), hj, vcJob)
			assert.NoError(t, err)

			// Check injected plugin args
			gotArgs, exists := vcJob.Spec.Plugins[VCJobPluginName]
			assert.True(t, exists, "PluginName should be injected into vcjob spec")
			assert.ElementsMatch(t, tt.expectedVCJobPluginArgs, gotArgs, "Args should be correctly passed down to the vcjob")

			// Check volumes and mounts
			podSpec := &vcJob.Spec.Tasks[0].Template.Spec
			volumes := make(map[string]corev1.Volume)
			for _, vol := range podSpec.Volumes {
				volumes[vol.Name] = vol
			}

			container := &podSpec.Containers[0]
			mounts := make(map[string]corev1.VolumeMount)
			for _, mount := range container.VolumeMounts {
				mounts[mount.Name] = mount
			}

			for resourceName, expectedPath := range tt.expectedMounts {
				// Check Volume exists
				vol, ok := volumes[resourceName]
				assert.True(t, ok, "Expected to find a volume named %s", resourceName)
				assert.NotNil(t, vol.VolumeSource.ConfigMap, "Volume source for %s should be a ConfigMap", resourceName)
				if vol.VolumeSource.ConfigMap != nil {
					expectedCMName := fmt.Sprintf("%s-%s", hj.Name, resourceName)
					assert.Equal(t, expectedCMName, vol.VolumeSource.ConfigMap.Name)
				}

				// Check VolumeMount exists and is correct
				mount, ok := mounts[resourceName]
				assert.True(t, ok, "Expected to find a volume mount named %s", resourceName)
				assert.Equal(t, expectedPath, mount.MountPath)
			}
		})
	}
}
