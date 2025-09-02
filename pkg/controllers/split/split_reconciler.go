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

package split

import (
	"context"
	"fmt"
	"reflect"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	batchv1alpha2 "volcano.sh/apis/pkg/apis/batch/v1alpha2"

	"volcano.sh/volcano-global/pkg/controllers/scheme"
)

const (
	HyperJobNameLabelKey = "volcano.sh/hyperjob-name"
	ReconcilerName       = "split-reconciler"
)

func init() {
	scheme.ReconcilerInitializers[ReconcilerName] = InitSplitReconciler
}

// SplitReconciler creates the corresponding number of vcjob and pp based on HyperJob, and aggregates the status of child vcjobs to HyperJob
type SplitReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func InitSplitReconciler(mgr ctrl.Manager) error {
	reconciler := NewSplitReconciler(mgr.GetClient(), mgr.GetScheme())
	return reconciler.SetupWithManager(mgr)
}

func NewSplitReconciler(client client.Client, scheme *runtime.Scheme) *SplitReconciler {
	return &SplitReconciler{
		Client: client,
		Scheme: scheme,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (s *SplitReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha2.HyperJob{}).
		Owns(&batchv1alpha2.Job{}).
		Owns(&policyv1alpha1.PropagationPolicy{}).
		Complete(s)
}

func (s *SplitReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.V(4).Infof("Reconciling HyperJob %s/%s", req.NamespacedName.Namespace, req.NamespacedName.Name)

	hyperJob := &batchv1alpha2.HyperJob{}
	if err := s.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, hyperJob); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if hyperJob.DeletionTimestamp != nil {
		// kube-controller-manager's gc-controller is responsible for deleting VCJobs and PPs
		return ctrl.Result{}, nil
	}

	if err := s.syncVCJobAndPP(ctx, hyperJob); err != nil {
		klog.Errorf("Failed to sync VCJob and PropagationPolicy for HyperJob %s/%s: %v", req.NamespacedName.Namespace, req.NamespacedName.Name, err)
		return ctrl.Result{}, err
	}

	if err := s.syncVCJobStatus(ctx, hyperJob); err != nil {
		klog.Errorf("Failed to sync VCJob status for HyperJob %s/%s: %v", req.NamespacedName.Namespace, req.NamespacedName.Name, err)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (s *SplitReconciler) syncVCJobAndPP(ctx context.Context, hyperJob *batchv1alpha2.HyperJob) error {
	childVCJobs := &batchv1alpha2.JobList{}
	selector := client.MatchingLabels(map[string]string{
		HyperJobNameLabelKey: hyperJob.Name,
	})
	if err := s.List(ctx, childVCJobs, client.InNamespace(hyperJob.Namespace), selector); err != nil {
		klog.Errorf("Failed to list child VCJobs for HyperJob %s/%s: %v", hyperJob.Namespace, hyperJob.Name, err)
		return err
	}
	childVCJobMap := make(map[string]batchv1alpha2.Job)
	for _, job := range childVCJobs.Items {
		childVCJobMap[job.Name] = job
	}

	childPPs := &policyv1alpha1.PropagationPolicyList{}
	if err := s.List(ctx, childPPs, client.InNamespace(hyperJob.Namespace), selector); err != nil {
		klog.Errorf("Failed to list child PropagationPolicies for HyperJob %s/%s: %v", hyperJob.Namespace, hyperJob.Name, err)
		return err
	}
	childPPMap := make(map[string]policyv1alpha1.PropagationPolicy)
	for _, pp := range childPPs.Items {
		childPPMap[pp.Name] = pp
	}

	for _, replicatedJob := range hyperJob.Spec.ReplicatedJobs {
		for i := 0; i < int(replicatedJob.Replicas); i++ {
			jobName := fmt.Sprintf("%s-%s-%d", hyperJob.Name, replicatedJob.Name, i)
			ppName := jobName

			// Reconcile VCJobs
			desiredVCJob, err := s.constructDesiredVCJob(hyperJob, &replicatedJob, jobName)
			if err != nil {
				klog.Errorf("Failed to construct desired VCJob for HyperJob %s/%s: %v", hyperJob.Namespace, hyperJob.Name, err)
				return err
			}

			if existingVCJob, exists := childVCJobMap[jobName]; !exists {
				klog.Infof("Creating a new VolcanoJob %s/%s", desiredVCJob.Namespace, desiredVCJob.Name)
				if err = s.Create(ctx, desiredVCJob); err != nil {
					klog.Errorf("Failed to create VolcanoJob %s/%s: %v", desiredVCJob.Namespace, desiredVCJob.Name, err)
					return err
				}
			} else {
				if !reflect.DeepEqual(desiredVCJob.Spec, existingVCJob.Spec) {
					klog.Infof("Updating existing VolcanoJob %s/%s", desiredVCJob.Namespace, desiredVCJob.Name)
					existingVCJob.Spec = desiredVCJob.Spec
					if err = s.Update(ctx, &existingVCJob); err != nil {
						klog.Errorf("Failed to update VolcanoJob %s/%s: %v", existingVCJob.Namespace, existingVCJob.Name, err)
						return err
					}
				}
				// Delete from map to mark as processed, then jobs left in the map are stale that need to be deleted
				delete(childVCJobMap, jobName)
			}

			// Reconcile PropagationPolicies
			desiredPP, err := s.constructDesiredPP(hyperJob, ppName)
			if err != nil {
				klog.Errorf("Failed to construct desired PropagationPolicy for HyperJob %s/%s: %v", hyperJob.Namespace, hyperJob.Name, err)
			}

			if existingPP, exists := childPPMap[ppName]; !exists {
				klog.Infof("Creating a new PropagationPolicy %s/%s", desiredPP.Namespace, desiredPP.Name)
				if err = s.Create(ctx, desiredPP); err != nil {
					klog.Errorf("Failed to create PropagationPolicy %s/%s: %v", desiredPP.Namespace, desiredPP.Name, err)
					return err
				}
			} else {
				if !reflect.DeepEqual(desiredPP.Spec, existingPP.Spec) {
					klog.Infof("Updating existing PropagationPolicy %s/%s", desiredPP.Namespace, desiredPP.Name)
					existingPP.Spec = desiredPP.Spec
					if err = s.Update(ctx, &existingPP); err != nil {
						klog.Errorf("Failed to update PropagationPolicy %s/%s: %v", existingPP.Namespace, existingPP.Name, err)
						return err
					}
				}
				// Delete from map to mark as processed, then PPs left in the map are stale that need to be deleted
				delete(childPPMap, ppName)
			}

		}
	}

	for _, staleJob := range childVCJobMap {
		klog.InfoS("Deleting stale VolcanoJob", "Name", staleJob.Name)
		if err := s.Delete(ctx, &staleJob); err != nil {
			return err
		}
	}
	for _, stalePP := range childPPMap {
		klog.InfoS("Deleting stale PropagationPolicy", "Name", stalePP.Name)
		if err := s.Delete(ctx, &stalePP); err != nil {
			return err
		}
	}

	return nil
}

func (s *SplitReconciler) constructDesiredVCJob(hyperJob *batchv1alpha2.HyperJob, replicatedJob *batchv1alpha2.ReplicatedJob, jobName string) (*batchv1alpha2.Job, error) {
	desiredVCJob := &batchv1alpha2.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: hyperJob.Namespace,
			Labels: map[string]string{
				HyperJobNameLabelKey: hyperJob.Name,
			},
		},
		Spec: replicatedJob.TemplateSpec,
	}

	if err := controllerutil.SetControllerReference(hyperJob, desiredVCJob, s.Scheme); err != nil {
		return nil, err
	}

	return desiredVCJob, nil
}

func (s *SplitReconciler) constructDesiredPP(hyperJob *batchv1alpha2.HyperJob, ppName string) (*policyv1alpha1.PropagationPolicy, error) {
	desiredPP := &policyv1alpha1.PropagationPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ppName,
			Namespace: hyperJob.Namespace,
			Labels: map[string]string{
				HyperJobNameLabelKey: hyperJob.Name,
			},
		},
		Spec: policyv1alpha1.PropagationSpec{
			ResourceSelectors: []policyv1alpha1.ResourceSelector{
				{
					APIVersion: batchv1alpha2.SchemeGroupVersion.String(),
					Kind:       "Job",
					Name:       ppName, // jobName is the same as ppName
				},
			},
			Placement: policyv1alpha1.Placement{
				// TODO: Add clusterAffinity from ReplicatedJob's preferences
				SpreadConstraints: []policyv1alpha1.SpreadConstraint{
					{
						SpreadByField: policyv1alpha1.SpreadByFieldCluster,
						MinGroups:     1,
						MaxGroups:     1,
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(hyperJob, desiredPP, s.Scheme); err != nil {
		return nil, err
	}

	return desiredPP, nil
}

func (s *SplitReconciler) syncVCJobStatus(ctx context.Context, hyperJob *batchv1alpha2.HyperJob) error {
	childVCJobs := &batchv1alpha2.JobList{}
	selector := client.MatchingLabels(map[string]string{
		HyperJobNameLabelKey: hyperJob.Name,
	})
	if err := s.List(ctx, childVCJobs, client.InNamespace(hyperJob.Namespace), selector); err != nil {
		klog.Errorf("Failed to list child VCJobs for HyperJob %s/%s: %v", hyperJob.Namespace, hyperJob.Name, err)
		return err
	}

	var newReplicatedJobsStatus []batchv1alpha2.ReplicatedJobStatus
	jobStatusMap := make(map[string]batchv1alpha2.ReplicatedJobStatus)
	for _, jobStatus := range hyperJob.Status.ReplicatedJobsStatus {
		jobStatusMap[jobStatus.Name] = jobStatus
	}

	for _, job := range childVCJobs.Items {
		if status, exists := jobStatusMap[job.Name]; exists {
			newReplicatedJobsStatus = append(newReplicatedJobsStatus, status)
		} else {
			newReplicatedJobsStatus = append(newReplicatedJobsStatus, batchv1alpha2.ReplicatedJobStatus{
				Name:      job.Name,
				JobStatus: job.Status,
			})
		}
	}

	if !reflect.DeepEqual(hyperJob.Status.ReplicatedJobsStatus, newReplicatedJobsStatus) {
		deepCopyHyperJob := hyperJob.DeepCopy()
		deepCopyHyperJob.Status.ReplicatedJobsStatus = newReplicatedJobsStatus
		if err := s.Status().Update(ctx, hyperJob); err != nil {
			klog.Errorf("Failed to update status for HyperJob %s/%s: %v", hyperJob.Namespace, hyperJob.Name, err)
			return err
		}
	}

	return nil
}
