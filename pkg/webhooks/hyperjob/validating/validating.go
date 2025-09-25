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
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	registrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
	"volcano.sh/volcano/pkg/webhooks/router"
	"volcano.sh/volcano/pkg/webhooks/util"

	"volcano.sh/volcano-global/pkg/webhooks/decoder"
)

func init() {
	router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path: "/hyperjobs/validate",
	Func: ValidateHyperJobResources,
	ValidatingConfig: &registrationv1.ValidatingWebhookConfiguration{
		Webhooks: []registrationv1.ValidatingWebhook{{
			Name: "validatehyperjobs.volcano.sh",
			Rules: []registrationv1.RuleWithOperations{
				{ // Rule for HyperJob
					Operations: []registrationv1.OperationType{registrationv1.Create, registrationv1.Update},
					Rule: registrationv1.Rule{
						APIGroups:   []string{trainingv1alpha1.SchemeGroupVersion.Group},
						APIVersions: []string{trainingv1alpha1.SchemeGroupVersion.Version},
						Resources:   []string{"hyperjobs"},
					},
				},
				{ // Rule for child VCJob
					Operations: []registrationv1.OperationType{registrationv1.Update},
					Rule: registrationv1.Rule{
						APIGroups:   []string{batchv1alpha1.SchemeGroupVersion.Group},
						APIVersions: []string{batchv1alpha1.SchemeGroupVersion.Version},
						Resources:   []string{"jobs"},
					},
				},
				{ // Rule for child PropagationPolicy
					Operations: []registrationv1.OperationType{registrationv1.Update},
					Rule: registrationv1.Rule{
						APIGroups:   []string{policyv1alpha1.GroupVersion.Group},
						APIVersions: []string{policyv1alpha1.GroupVersion.Version},
						Resources:   []string{"propagationpolicies"},
					},
				},
			},
		}},
	},
}

// ValidateHyperJobResources is the main handler function for the webhook.
func ValidateHyperJobResources(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	if ar.Request == nil {
		return util.ToAdmissionResponse(fmt.Errorf("empty request"))
	}

	switch ar.Request.Kind.Kind {
	case "HyperJob":
		return handleHyperJob(ar)
	case "Job":
		return handleChildResourceUpdate(ar, "Job")
	case "PropagationPolicy":
		return handleChildResourceUpdate(ar, "PropagationPolicy")
	default:
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
}

// handleHyperJob handles validation for HyperJob.
func handleHyperJob(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	switch ar.Request.Operation {
	case admissionv1.Create:
		hj, err := decoder.DecodeHyperJob(ar.Request.Object, ar.Request.Resource)
		if err != nil {
			return util.ToAdmissionResponse(err)
		}
		return validateHyperJobCreate(hj)
	case admissionv1.Update:
		oldHj, err := decoder.DecodeHyperJob(ar.Request.OldObject, ar.Request.Resource)
		if err != nil {
			return util.ToAdmissionResponse(err)
		}
		newHj, err := decoder.DecodeHyperJob(ar.Request.Object, ar.Request.Resource)
		if err != nil {
			return util.ToAdmissionResponse(err)
		}
		return validateHyperJobUpdate(oldHj, newHj)
	default:
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
}

// validateHyperJobCreate validates the creation of a HyperJob.
func validateHyperJobCreate(hj *trainingv1alpha1.HyperJob) *admissionv1.AdmissionResponse {
	// 1. Validate that HyperJob and ReplicatedJob names are valid DNS-1123 labels,
	// as they are used as label values. This enforces the 63 character limit.
	if errs := validation.IsDNS1123Label(hj.Name); len(errs) > 0 {
		return util.ToAdmissionResponse(fmt.Errorf("invalid hyperjob name %s: %s", hj.Name, strings.Join(errs, "; ")))
	}

	for _, rj := range hj.Spec.ReplicatedJobs {
		if errs := validation.IsDNS1123Label(rj.Name); len(errs) > 0 {
			return util.ToAdmissionResponse(fmt.Errorf("invalid replicatedjob name %s: %s", rj.Name, strings.Join(errs, "; ")))
		}

		for _, task := range rj.TemplateSpec.Tasks {
			// 2. Validate the worst-case name for the final Pod.
			// Currently, member cluster has a `/jobs/validate` webhook to validate the length of a pod name,
			// therefore we also use IsQualifiedName validation here to ensure the generated pod name and length is valid.
			worstCasePodName := fmt.Sprintf("%s-%s-%d-%s-%d", hj.Name, rj.Name, rj.Replicas-1, task.Name, task.Replicas-1)
			if errs := validation.IsQualifiedName(worstCasePodName); len(errs) > 0 {
				return util.ToAdmissionResponse(fmt.Errorf(
					"the generated pod name would be invalid: %s. (Worst-case pod name example: %s)",
					strings.Join(errs, "; "), worstCasePodName,
				))
			}
		}
	}

	return &admissionv1.AdmissionResponse{Allowed: true}
}

// validateHyperJobUpdate validates the update of a HyperJob
func validateHyperJobUpdate(oldHj, newHj *trainingv1alpha1.HyperJob) *admissionv1.AdmissionResponse {
	// Currently, hyperjob spec is immutable, we will gradually open and allow updating some fields in the future
	if diff := cmp.Diff(oldHj.Spec, newHj.Spec); diff != "" {
		return util.ToAdmissionResponse(fmt.Errorf("hyperjob spec is immutable; detected changes:\n%s", diff))
	}
	return &admissionv1.AdmissionResponse{Allowed: true}
}

// handleChildResourceUpdate prevents spec updates to child resources owned by a HyperJob.
func handleChildResourceUpdate(ar admissionv1.AdmissionReview, kind string) *admissionv1.AdmissionResponse {
	var oldSpec, newSpec interface{}
	var oldObj metav1.Object

	switch kind {
	case "Job":
		oldJob, err := decoder.DecodeVolcanoJob(ar.Request.OldObject, ar.Request.Resource)
		if err != nil {
			return util.ToAdmissionResponse(err)
		}
		newJob, err := decoder.DecodeVolcanoJob(ar.Request.Object, ar.Request.Resource)
		if err != nil {
			return util.ToAdmissionResponse(err)
		}
		oldSpec, newSpec = oldJob.Spec, newJob.Spec
		oldObj = oldJob
	case "PropagationPolicy":
		oldPP, errDecodeOld := decoder.DecodePropagationPolicy(ar.Request.OldObject, ar.Request.Resource)
		if errDecodeOld != nil {
			return util.ToAdmissionResponse(errDecodeOld)
		}
		newPP, errDecodeNew := decoder.DecodePropagationPolicy(ar.Request.Object, ar.Request.Resource)
		if errDecodeNew != nil {
			return util.ToAdmissionResponse(errDecodeNew)
		}
		oldSpec, newSpec = oldPP.Spec, newPP.Spec
		oldObj = oldPP
	default:
		return util.ToAdmissionResponse(fmt.Errorf("unsupported kind %s for child resource validation, expect Job or PropagationPolicy", kind))
	}

	owner := metav1.GetControllerOf(oldObj)
	if owner != nil && owner.APIVersion == trainingv1alpha1.SchemeGroupVersion.String() && owner.Kind == "HyperJob" {
		if diff := cmp.Diff(oldSpec, newSpec); diff != "" {
			return util.ToAdmissionResponse(fmt.Errorf(
				"cannot directly update spec of %s %q because it is owned and managed by HyperJob %q; detected changes:\n%s",
				kind, oldObj.GetName(), owner.Name, diff,
			))
		}
	}

	return &admissionv1.AdmissionResponse{Allowed: true}
}
