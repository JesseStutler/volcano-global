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

package validating

import (
	"encoding/json"
	"strings"
	"testing"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"github.com/stretchr/testify/assert"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
	"volcano.sh/volcano-global/pkg/webhooks/decoder"
)

func TestValidateHyperJobCreate(t *testing.T) {
	longName64 := strings.Repeat("a", 64)

	validRJ := trainingv1alpha1.ReplicatedJob{
		Name:     "worker",
		Replicas: 1,
		TemplateSpec: batchv1alpha1.JobSpec{
			Tasks: []batchv1alpha1.TaskSpec{{Name: "task", Replicas: 1}},
		},
	}

	tests := []struct {
		name        string
		hyperJob    *trainingv1alpha1.HyperJob
		expectAllow bool
		errorMsg    string
	}{
		{
			name: "Success: valid hyperjob",
			hyperJob: &trainingv1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{Name: "hj-good"},
				Spec:       trainingv1alpha1.HyperJobSpec{ReplicatedJobs: []trainingv1alpha1.ReplicatedJob{validRJ}},
			},
			expectAllow: true,
		},
		{
			name: "Failure: hyperjob name too long (>63)",
			hyperJob: &trainingv1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{Name: longName64},
				Spec:       trainingv1alpha1.HyperJobSpec{ReplicatedJobs: []trainingv1alpha1.ReplicatedJob{validRJ}},
			},
			expectAllow: false,
			errorMsg:    "invalid hyperjob name",
		},
		{
			name: "Failure: hyperjob name has invalid chars",
			hyperJob: &trainingv1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{Name: "hj-bad.name"},
				Spec:       trainingv1alpha1.HyperJobSpec{ReplicatedJobs: []trainingv1alpha1.ReplicatedJob{validRJ}},
			},
			expectAllow: false,
			errorMsg:    "invalid hyperjob name",
		},
		{
			name: "Failure: replicatedjob name too long",
			hyperJob: &trainingv1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{Name: "hj-ok"},
				Spec: trainingv1alpha1.HyperJobSpec{ReplicatedJobs: []trainingv1alpha1.ReplicatedJob{
					{Name: longName64, Replicas: 1, TemplateSpec: validRJ.TemplateSpec},
				}},
			},
			expectAllow: false,
			errorMsg:    "invalid replicatedjob name",
		},
		{
			name: "Failure: task name has invalid chars",
			hyperJob: &trainingv1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{Name: "hj-ok"},
				Spec: trainingv1alpha1.HyperJobSpec{ReplicatedJobs: []trainingv1alpha1.ReplicatedJob{
					{Name: "worker", Replicas: 1, TemplateSpec: batchv1alpha1.JobSpec{
						Tasks: []batchv1alpha1.TaskSpec{{Name: "task!bad", Replicas: 1}},
					}},
				}},
			},
			expectAllow: false,
			errorMsg:    "generated pod name would be invalid",
		},
		{
			name: "Failure: final pod name too long",
			hyperJob: &trainingv1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{Name: "hyperjob"},
				Spec: trainingv1alpha1.HyperJobSpec{ReplicatedJobs: []trainingv1alpha1.ReplicatedJob{
					{Name: "replicatedjob", Replicas: 1, TemplateSpec: batchv1alpha1.JobSpec{
						Tasks: []batchv1alpha1.TaskSpec{{Name: longName64, Replicas: 1}},
					}},
				}},
			},
			expectAllow: false,
			errorMsg:    "generated pod name would be invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := validateHyperJobCreate(tt.hyperJob)
			assert.Equal(t, tt.expectAllow, response.Allowed)
			if !tt.expectAllow {
				assert.Contains(t, response.Result.Message, tt.errorMsg)
			}
		})
	}
}

func TestValidateHyperJobUpdate(t *testing.T) {
	baseHJ := &trainingv1alpha1.HyperJob{
		ObjectMeta: metav1.ObjectMeta{Name: "test-hj"},
		Spec: trainingv1alpha1.HyperJobSpec{
			ReplicatedJobs: []trainingv1alpha1.ReplicatedJob{{Name: "worker", Replicas: 2}},
		},
	}

	tests := []struct {
		name        string
		oldHJ       *trainingv1alpha1.HyperJob
		newHJ       *trainingv1alpha1.HyperJob
		expectAllow bool
		errorMsg    string
	}{
		{
			name:        "Success: spec is unchanged",
			oldHJ:       baseHJ,
			newHJ:       baseHJ.DeepCopy(),
			expectAllow: true,
		},
		{
			name:  "Failure: spec is changed",
			oldHJ: baseHJ,
			newHJ: func() *trainingv1alpha1.HyperJob {
				hj := baseHJ.DeepCopy()
				hj.Spec.ReplicatedJobs[0].Replicas = 3 // Change a field
				return hj
			}(),
			expectAllow: false,
			errorMsg:    "hyperjob spec is immutable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := validateHyperJobUpdate(tt.oldHJ, tt.newHJ)
			assert.Equal(t, tt.expectAllow, response.Allowed)
			if !tt.expectAllow {
				assert.Contains(t, response.Result.Message, tt.errorMsg)
			}
		})
	}
}

func TestHandleChildResourceUpdate(t *testing.T) {
	ownerRef := metav1.OwnerReference{
		APIVersion: trainingv1alpha1.SchemeGroupVersion.String(),
		Kind:       "HyperJob",
		Name:       "owner-hj",
		Controller: ptr.To(true),
	}

	baseVCJob := &batchv1alpha1.Job{
		TypeMeta:   metav1.TypeMeta{Kind: "Job", APIVersion: batchv1alpha1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: "child-vcjob", OwnerReferences: []metav1.OwnerReference{ownerRef}},
		Spec:       batchv1alpha1.JobSpec{MinAvailable: 1},
	}

	basePP := &policyv1alpha1.PropagationPolicy{
		TypeMeta:   metav1.TypeMeta{Kind: "PropagationPolicy", APIVersion: policyv1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: "child-pp", OwnerReferences: []metav1.OwnerReference{ownerRef}},
		Spec:       policyv1alpha1.PropagationSpec{PropagateDeps: true},
	}

	tests := []struct {
		name        string
		resource    metav1.GroupVersionResource
		kind        string
		oldObject   metav1.Object
		newObject   metav1.Object
		expectAllow bool
		errorMsg    string
	}{
		{
			name:      "Failure: update spec of owned vcjob",
			resource:  decoder.VolcanoJobGVR,
			kind:      "Job",
			oldObject: baseVCJob,
			newObject: func() *batchv1alpha1.Job {
				job := baseVCJob.DeepCopy()
				job.Spec.MinAvailable = 2 // Change spec
				return job
			}(),
			expectAllow: false,
			errorMsg:    "cannot directly update spec of Job",
		},
		{
			name:      "Failure: update spec of owned pp",
			resource:  decoder.PropagationPolicyGVR,
			kind:      "PropagationPolicy",
			oldObject: basePP,
			newObject: func() *policyv1alpha1.PropagationPolicy {
				pp := basePP.DeepCopy()
				pp.Spec.PropagateDeps = false // Change spec
				return pp
			}(),
			expectAllow: false,
			errorMsg:    "cannot directly update spec of PropagationPolicy",
		},
		{
			name:      "Success: update metadata of owned vcjob",
			resource:  decoder.VolcanoJobGVR,
			kind:      "Job",
			oldObject: baseVCJob,
			newObject: func() *batchv1alpha1.Job {
				job := baseVCJob.DeepCopy()
				job.Finalizers = []string{"new-finalizer"} // Change metadata
				return job
			}(),
			expectAllow: true,
		},
		{
			name:      "Success: update metadata of owned pp",
			resource:  decoder.PropagationPolicyGVR,
			kind:      "PropagationPolicy",
			oldObject: basePP,
			newObject: func() *policyv1alpha1.PropagationPolicy {
				pp := basePP.DeepCopy()
				pp.Finalizers = []string{"new-finalizer"} // Change metadata
				return pp
			}(),
			expectAllow: true,
		},
		{
			name:     "Success: update spec of un-owned vcjob",
			resource: decoder.VolcanoJobGVR,
			kind:     "Job",
			oldObject: &batchv1alpha1.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job", APIVersion: batchv1alpha1.SchemeGroupVersion.String()},
				ObjectMeta: metav1.ObjectMeta{Name: "unrelated-vcjob"},
				Spec:       batchv1alpha1.JobSpec{MinAvailable: 1},
			},
			newObject: &batchv1alpha1.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job", APIVersion: batchv1alpha1.SchemeGroupVersion.String()},
				ObjectMeta: metav1.ObjectMeta{Name: "unrelated-vcjob"},
				Spec:       batchv1alpha1.JobSpec{MinAvailable: 2}, // Change spec
			},
			expectAllow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldRaw, err := json.Marshal(tt.oldObject)
			assert.NoError(t, err)
			newRaw, err := json.Marshal(tt.newObject)
			assert.NoError(t, err)

			ar := admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Group:   tt.resource.Group,
						Version: tt.resource.Version,
						Kind:    tt.kind,
					},
					Resource:  tt.resource,
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{Raw: oldRaw},
					Object:    runtime.RawExtension{Raw: newRaw},
				},
			}

			// Call the main handler, which includes decoding
			response := ValidateHyperJobResources(ar)
			assert.Equal(t, tt.expectAllow, response.Allowed)
			if !tt.expectAllow {
				assert.Contains(t, response.Result.Message, tt.errorMsg)
			}
		})
	}
}
