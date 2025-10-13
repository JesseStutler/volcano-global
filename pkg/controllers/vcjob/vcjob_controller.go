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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/volcano-global/pkg/controllers/scheme"
)

const (
	ReconcilerName = "vcjob-controller"
)

func init() {
	scheme.ReconcilerInitializers[ReconcilerName] = InitVCJobController
}

// VCJobController is used to refresh the default status for vcjobs created by hyperjobs
type VCJobController struct {
	client.Client
	Scheme *runtime.Scheme
}

func InitVCJobController(mgr ctrl.Manager) error {
	reconciler := NewVCJobController(mgr.GetClient(), mgr.GetScheme())
	return reconciler.SetupWithManager(mgr)
}

func NewVCJobController(client client.Client, scheme *runtime.Scheme) *VCJobController {
	return &VCJobController{
		Client: client,
		Scheme: scheme,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (v *VCJobController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.Job{}).
		Complete(v)
}

func (v *VCJobController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	job := &batchv1alpha1.Job{}
	if err := v.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if job.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	// If the job status phase is already set, no need to initialize it again
	if job.Status.State.Phase != "" {
		return ctrl.Result{}, nil
	}

	// Initialize the default status for the VCJob, the logic of status update is referred from "volcano/pkg/controllers/job/job_controller_actions.go"
	updatedJob := job.DeepCopy()
	updatedJob.Status.State.Phase = batchv1alpha1.Pending
	updatedJob.Status.State.LastTransitionTime = metav1.Now()
	updatedJob.Status.MinAvailable = job.Spec.MinAvailable
	updatedJob.Status.Running = 0
	jobCondition := newJobCondition(updatedJob.Status.State.Phase, &updatedJob.Status.State.LastTransitionTime)
	updatedJob.Status.Conditions = append(updatedJob.Status.Conditions, jobCondition)
	if err := v.Status().Update(ctx, updatedJob); err != nil {
		log.Error(err, "Failed to update status of VCJob")
		return ctrl.Result{}, err
	}

	log.V(4).Info("Successfully initialized default status for VCJob")
	return ctrl.Result{}, nil
}

// newJobCondition creates a new JobCondition based on the given phase and transition time
func newJobCondition(phase batchv1alpha1.JobPhase, lastTransitionTime *metav1.Time) batchv1alpha1.JobCondition {
	return batchv1alpha1.JobCondition{
		Status:             phase,
		LastTransitionTime: lastTransitionTime,
	}
}
