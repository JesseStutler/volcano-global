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

package utils

const (
	HyperJobNameLabelKey = "volcano.sh/hyperjob-name"
	// ReplicatedJobNameLabelKey is used to identify which ReplicatedJob a VCJob belongs to.
	// It is convenient for controller to query the replicatedJob a VCJob belongs to and aggregate status
	ReplicatedJobNameLabelKey = "volcano.sh/replicatedjob-name"
	// Labels for storing the hash of the user's original vcjob templateSpec/ppSpec
	VCJobTemplateSpecHashLabelKey = "volcano.sh/vcjob-template-spec-hash"
	PPSpecHashLabelKey            = "volcano.sh/pp-spec-hash"
)
