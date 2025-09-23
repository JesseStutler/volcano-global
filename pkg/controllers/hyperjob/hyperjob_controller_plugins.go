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

package hyperjob

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
	hjplugins "volcano.sh/volcano-global/pkg/controllers/hyperjob/plugins/scheme"
)

// pluginOnJobCreate will execute all plugins' OnJobCreate method and modify the job accordingly.
func (h *HyperJobController) pluginOnJobCreate(ctx context.Context, hj *trainingv1alpha1.HyperJob, job *batchv1alpha1.Job) error {
	log := ctrl.LoggerFrom(ctx)

	for name, args := range hj.Spec.Plugins {
		builder, found := hjplugins.GetPluginBuilder(name)
		if !found {
			return fmt.Errorf("plugin %s not found", name)
		}

		plugin, err := builder(h.Client, args)
		if err != nil {
			return fmt.Errorf("failed to build plugin %s: %v", name, err)
		}

		log.V(4).Info("Starting to execute plugin at <pluginOnJobCreate>", "plugin", name)
		if err = plugin.OnJobCreate(ctx, hj, job); err != nil {
			return fmt.Errorf("plugin %s OnJobCreate failed: %v", name, err)
		}
	}

	return nil
}

// pluginOnHyperJobAdd will execute all plugins' OnHyperJobAdd method in hyperjob initialization.
func (h *HyperJobController) pluginOnHyperJobAdd(ctx context.Context, hj *trainingv1alpha1.HyperJob) error {
	log := ctrl.LoggerFrom(ctx)

	for name, args := range hj.Spec.Plugins {
		builder, found := hjplugins.GetPluginBuilder(name)
		if !found {
			return fmt.Errorf("plugin %s not found", name)
		}

		plugin, err := builder(h.Client, args)
		if err != nil {
			return fmt.Errorf("failed to build plugin %s: %v", name, err)
		}

		log.V(4).Info("Starting to execute plugin at <pluginOnHyperJobAdd>", "plugin", name)
		if err = plugin.OnHyperJobAdd(ctx, hj); err != nil {
			return fmt.Errorf("plugin %s OnJobCreate failed: %v", name, err)
		}
	}

	return nil
}
