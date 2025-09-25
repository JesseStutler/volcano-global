/*
Copyright 2024 The Volcano Authors.

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

package decoder

import (
	"fmt"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
)

func init() {
	addToScheme(scheme)
}

var scheme = runtime.NewScheme()

// codecs for retrieving serializers for the supported wire formats
// and conversion wrappers to define preferred internal and external versions.
var codecs = serializer.NewCodecFactory(scheme)

func addToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(admissionv1.AddToScheme(scheme))
	utilruntime.Must(trainingv1alpha1.AddToScheme(scheme))
	utilruntime.Must(batchv1alpha1.AddToScheme(scheme))
	utilruntime.Must(policyv1alpha1.AddToScheme(scheme))
}

var ResourceBindingGVR = metav1.GroupVersionResource{
	Group:    workv1alpha2.GroupVersion.Group,
	Version:  workv1alpha2.GroupVersion.Version,
	Resource: workv1alpha2.ResourcePluralResourceBinding,
}

var HyperJobGVR = metav1.GroupVersionResource{
	Group:    trainingv1alpha1.SchemeGroupVersion.Group,
	Version:  trainingv1alpha1.SchemeGroupVersion.Version,
	Resource: "hyperjobs",
}

var VolcanoJobGVR = metav1.GroupVersionResource{
	Group:    batchv1alpha1.SchemeGroupVersion.Group,
	Version:  batchv1alpha1.SchemeGroupVersion.Version,
	Resource: "jobs",
}

var PropagationPolicyGVR = metav1.GroupVersionResource{
	Group:    policyv1alpha1.GroupVersion.Group,
	Version:  policyv1alpha1.GroupVersion.Version,
	Resource: "propagationpolicies",
}

// DecodeResourceBinding decode the ResourceBinding use deserializer from the raw object.
func DecodeResourceBinding(object runtime.RawExtension, gvr metav1.GroupVersionResource) (*workv1alpha2.ResourceBinding, error) {
	if gvr != ResourceBindingGVR {
		return nil, fmt.Errorf("expect resource to be %s", ResourceBindingGVR)
	}

	deserializer := codecs.UniversalDeserializer()
	resourceBinding := &workv1alpha2.ResourceBinding{}
	if _, _, err := deserializer.Decode(object.Raw, nil, resourceBinding); err != nil {
		return nil, err
	}

	klog.V(5).Infof("The ResourceBinding struct is %+v", resourceBinding)
	return resourceBinding, nil
}

// DecodeHyperJob decodes the HyperJob from a raw object.
func DecodeHyperJob(object runtime.RawExtension, gvr metav1.GroupVersionResource) (*trainingv1alpha1.HyperJob, error) {
	if gvr != HyperJobGVR {
		return nil, fmt.Errorf("expect resource to be %s", HyperJobGVR)
	}

	deserializer := codecs.UniversalDeserializer()
	hyperJob := &trainingv1alpha1.HyperJob{}
	if _, _, err := deserializer.Decode(object.Raw, nil, hyperJob); err != nil {
		return nil, err
	}

	return hyperJob, nil
}

// DecodeVolcanoJob decodes the Volcano Job from a raw object.
func DecodeVolcanoJob(object runtime.RawExtension, gvr metav1.GroupVersionResource) (*batchv1alpha1.Job, error) {
	if gvr != VolcanoJobGVR {
		return nil, fmt.Errorf("expect resource to be %s", VolcanoJobGVR)
	}

	deserializer := codecs.UniversalDeserializer()
	job := &batchv1alpha1.Job{}
	if _, _, err := deserializer.Decode(object.Raw, nil, job); err != nil {
		return nil, err
	}

	return job, nil
}

// DecodePropagationPolicy decodes the PropagationPolicy from a raw object.
func DecodePropagationPolicy(object runtime.RawExtension, gvr metav1.GroupVersionResource) (*policyv1alpha1.PropagationPolicy, error) {
	if gvr != PropagationPolicyGVR {
		return nil, fmt.Errorf("expect resource to be %s", PropagationPolicyGVR)
	}

	deserializer := codecs.UniversalDeserializer()
	policy := &policyv1alpha1.PropagationPolicy{}
	if _, _, err := deserializer.Decode(object.Raw, nil, policy); err != nil {
		return nil, err
	}

	return policy, nil
}
