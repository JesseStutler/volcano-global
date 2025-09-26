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

package pluginsinterface

import (
	"context"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
)

type PluginInterface interface {
	// Name returns the unique name of the plugin
	Name() string

	// OnHyperJobAdd is called when do hyperjob initialization
	OnHyperJobAdd(ctx context.Context, hj *trainingv1alpha1.HyperJob) error

	// OnJobCreate is called when creating jobs
	OnJobCreate(ctx context.Context, hj *trainingv1alpha1.HyperJob, job *batchv1alpha1.Job) error
}
