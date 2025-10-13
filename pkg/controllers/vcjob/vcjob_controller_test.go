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

package vcjob

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

func setupTestController() (*VCJobController, client.Client) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = batchv1alpha1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&batchv1alpha1.Job{}).
		Build()

	controller := NewVCJobController(fakeClient, scheme)

	return controller, fakeClient
}

func createTestVCJob(name, namespace string, phase batchv1alpha1.JobPhase, minAvailable int32) *batchv1alpha1.Job {
	job := &batchv1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: batchv1alpha1.JobSpec{
			MinAvailable: minAvailable,
			Tasks: []batchv1alpha1.TaskSpec{
				{
					Name:     "worker",
					Replicas: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "worker",
									Image: "test-image",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("128Mi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if phase != "" {
		now := metav1.Now()
		job.Status = batchv1alpha1.JobStatus{
			State: batchv1alpha1.JobState{
				Phase:              phase,
				LastTransitionTime: now,
			},
			MinAvailable: minAvailable,
			Conditions: []batchv1alpha1.JobCondition{
				{
					Status:             phase,
					LastTransitionTime: &now,
				},
			},
		}
	}

	return job
}

func TestReconcile(t *testing.T) {
	namespace := "test-ns"

	tests := []struct {
		name                    string
		job                     *batchv1alpha1.Job
		expectStatusInitialized bool
		expectPhase             batchv1alpha1.JobPhase
	}{
		{
			name:                    "Job already has status - should not reinitialize",
			job:                     createTestVCJob("job-with-status", namespace, batchv1alpha1.Running, 2),
			expectStatusInitialized: false,
			expectPhase:             batchv1alpha1.Running,
		},
		{
			name:                    "Job without status - should initialize to Pending",
			job:                     createTestVCJob("job-without-status", namespace, "", 3),
			expectStatusInitialized: true,
			expectPhase:             batchv1alpha1.Pending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, fakeClient := setupTestController()
			ctx := context.Background()

			err := fakeClient.Create(ctx, tt.job)
			assert.NoError(t, err)

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      tt.job.Name,
					Namespace: tt.job.Namespace,
				},
			}

			result, err := controller.Reconcile(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, reconcile.Result{}, result)

			updatedJob := &batchv1alpha1.Job{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      tt.job.Name,
				Namespace: tt.job.Namespace,
			}, updatedJob)
			assert.NoError(t, err)

			assert.Equal(t, tt.expectPhase, updatedJob.Status.State.Phase, "Phase should be initialized to Pending")
			assert.NotNil(t, updatedJob.Status.State.LastTransitionTime, "LastTransitionTime should be set")
			assert.Equal(t, tt.job.Spec.MinAvailable, updatedJob.Status.MinAvailable, "MinAvailable should be copied from Spec")
			assert.Equal(t, int32(0), updatedJob.Status.Running, "Running should be initialized to 0")
			assert.Len(t, updatedJob.Status.Conditions, 1, "Should have one condition")

			condition := updatedJob.Status.Conditions[0]
			assert.Equal(t, tt.expectPhase, condition.Status, "Condition status should match phase")
			assert.NotNil(t, condition.LastTransitionTime, "Condition LastTransitionTime should be set")
		})
	}
}
