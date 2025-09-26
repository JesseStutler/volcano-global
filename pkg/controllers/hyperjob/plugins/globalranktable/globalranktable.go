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

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
	"volcano.sh/volcano-global/pkg/controllers/hyperjob/utils"

	pluginsinterface "volcano.sh/volcano-global/pkg/controllers/hyperjob/plugins/interface"
)

const (
	PluginName = "globalranktable"
	// VCJobPluginName is the name of the globalranktable plugin in vcjob, currently still named as configmap1980 for compatibility
	VCJobPluginName = "configmap1980"
	// The names of the configmaps created by globalranktable plugin
	ConfigmapGlobalRankTable    = "global-ranktable"
	ConfigmapGlobalNetWorkLinks = "global-network-links"
	// The env keys of mount path
	GlobalRankTableFilePath    = "GLOBAL_RANK_TABLE_FILE"
	GlobalNetworkLinksFilePath = "GLOBAL_NETWORK_LINKS"

	DefaultMountPrefix            = "/user/config"
	DefaultGlobalRankTableVersion = "1.4"

	AcceleratePolicyPSM = "psm"
)

var defaultConfigmapsNeedToCreate = []string{
	ConfigmapGlobalRankTable,
	ConfigmapGlobalNetWorkLinks,
}

type GlobalRanktablePlugin struct {
	client client.Client
	args   *GlobalRanktablePluginArgs
}

type GlobalRanktablePluginArgs struct {
	RankTableVersion   string
	AIAcceleratePolicy string
	ConfigmapsToCreate []string
}

func NewPlugin(client client.Client, args []string) (pluginsinterface.PluginInterface, error) {
	parsedArgs, err := parsePluginArgs(args)
	if err != nil {
		return nil, err
	}

	if err := validatePluginArgs(parsedArgs); err != nil {
		return nil, err
	}

	return &GlobalRanktablePlugin{
		client: client,
		args:   parsedArgs,
	}, nil
}

func parsePluginArgs(args []string) (*GlobalRanktablePluginArgs, error) {
	pa := &GlobalRanktablePluginArgs{}
	flagSet := pflag.NewFlagSet(PluginName, pflag.ContinueOnError)

	flagSet.StringVar(&pa.RankTableVersion, "rank-table-version", DefaultGlobalRankTableVersion, "The version of ranktable to use, default is 1.4.")
	flagSet.StringSliceVar(&pa.ConfigmapsToCreate, "configmaps-to-create", defaultConfigmapsNeedToCreate, "The configmaps to create for globalranktable plugin, default is [global-ranktable, global-network-links].")
	flagSet.StringVar(&pa.AIAcceleratePolicy, "ai-accelerate-policy", "", "The AI accelerate policy to use, e.g., psm. If not set, the ranktable will be generated without considering AI accelerator topology.")

	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("failed to parse plugin args: %v", err)
	}

	return pa, nil
}

func validatePluginArgs(args *GlobalRanktablePluginArgs) error {
	validPolicies := map[string]bool{
		"":                  true,
		AcceleratePolicyPSM: true,
	}

	if !validPolicies[args.AIAcceleratePolicy] {
		return fmt.Errorf("invalid ai-accelerate-policy %q, only 'psm' or empty is supported", args.AIAcceleratePolicy)
	}

	// Future validations for other arguments can be added here.
	return nil
}

func (grp *GlobalRanktablePlugin) Name() string {
	return PluginName
}

func (grp *GlobalRanktablePlugin) OnHyperJobAdd(ctx context.Context, hj *trainingv1alpha1.HyperJob) error {
	log := ctrl.LoggerFrom(ctx)

	for _, cmName := range grp.args.ConfigmapsToCreate {
		desiredCM, err := grp.constructDesiredConfigmap(hj, cmName)
		if err != nil {
			return fmt.Errorf("failed to construct desired configmap %s: %v", cmName, err)
		}

		cm := &corev1.ConfigMap{}
		err = grp.client.Get(ctx, types.NamespacedName{Name: desiredCM.Name, Namespace: desiredCM.Namespace}, cm)
		if err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to get configmap %s: %v", desiredCM.Name, err)
			}
			// Not found, create it
			if err = grp.client.Create(ctx, desiredCM); err != nil {
				return fmt.Errorf("failed to create configmap %s: %v", desiredCM.Name, err)
			}
			log.Info("Created configmap for globalranktable plugin", "configmap", desiredCM.Name)
		}
	}

	return nil
}

func (grp *GlobalRanktablePlugin) OnJobCreate(ctx context.Context, hj *trainingv1alpha1.HyperJob, job *batchv1alpha1.Job) error {
	log := ctrl.LoggerFrom(ctx)

	// When globalranktable plugin is enabled for a HyperJob, the vcjob created by the HyperJob should also contain the globalranktable plugin.
	if job.Spec.Plugins == nil {
		job.Spec.Plugins = make(map[string][]string)
	}
	// The args of globalranktable plugin in vcjob is the same as that in hyperjob. But if vcjob already has the globalranktable plugin, we do not overwrite it,
	// only log a warning.
	if _, exists := job.Spec.Plugins[VCJobPluginName]; exists {
		log.Info("Warning: vcjob already has the globalranktable plugin, will not overwrite it.", "vcjob", job.Name)
	} else {
		args := []string{
			fmt.Sprintf("--rank-table-version=%s", grp.args.RankTableVersion),
		}
		if grp.args.AIAcceleratePolicy == AcceleratePolicyPSM {
			args = append(args, "--rank-table-tor-enable=true")
		}
		job.Spec.Plugins[VCJobPluginName] = args
	}

	for _, resource := range grp.args.ConfigmapsToCreate {
		cmName := fmt.Sprintf("%s-%s", hj.Name, resource)

		volume := corev1.Volume{
			Name: resource,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cmName,
					},
				},
			},
		}

		// Add the volume and volume mount to each task in the vcjob spec.
		for i := range job.Spec.Tasks {
			taskPodSpec := &job.Spec.Tasks[i].Template.Spec
			if taskPodSpec.Volumes == nil {
				taskPodSpec.Volumes = make([]corev1.Volume, 0)
			}
			taskPodSpec.Volumes = append(taskPodSpec.Volumes, volume)

			for j := range taskPodSpec.Containers {
				container := &taskPodSpec.Containers[j]
				if container.VolumeMounts == nil {
					container.VolumeMounts = make([]corev1.VolumeMount, 0)
				}

				volumeMount := corev1.VolumeMount{
					Name:     resource,
					ReadOnly: true,
				}
				for _, env := range container.Env {
					if resource == ConfigmapGlobalRankTable && env.Name == GlobalRankTableFilePath {
						volumeMount.MountPath = env.Value
					}
					if resource == ConfigmapGlobalNetWorkLinks && env.Name == GlobalNetworkLinksFilePath {
						volumeMount.MountPath = env.Value
					}
				}
				if volumeMount.MountPath == "" {
					// If the user does not specify the mount path through env, use the default mount path.
					volumeMount.MountPath = fmt.Sprintf("%s/%s", DefaultMountPrefix, resource)
				}
				container.VolumeMounts = append(container.VolumeMounts, volumeMount)
			}
		}
	}

	return nil
}

func (grp *GlobalRanktablePlugin) constructDesiredConfigmap(hj *trainingv1alpha1.HyperJob, resourceSuffixName string) (*corev1.ConfigMap, error) {
	desiredCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: hj.Namespace,
			Name:      fmt.Sprintf("%s-%s", hj.Name, resourceSuffixName),
			Labels: map[string]string{
				utils.HyperJobNameLabelKey: hj.Name,
			},
		},
	}

	if err := controllerutil.SetControllerReference(hj, desiredCM, grp.client.Scheme()); err != nil {
		return nil, err
	}

	return desiredCM, nil
}
