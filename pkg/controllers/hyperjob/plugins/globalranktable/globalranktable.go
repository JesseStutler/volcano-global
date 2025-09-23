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

	DefaultMountPrefix = "/user/config"
)

var configmapsNeedToCreate = []string{
	ConfigmapGlobalRankTable,
	ConfigmapGlobalNetWorkLinks,
}

type GlobalRanktablePlugin struct {
	client client.Client
	args   []string
}

func NewPlugin(client client.Client, args []string) (pluginsinterface.PluginInterface, error) {
	// Currently we do not parse and validate the args of globalranktable plugin in hyperjob, only directly pass them to vcjob.
	return &GlobalRanktablePlugin{
		client: client,
		args:   args,
	}, nil
}

func (grp *GlobalRanktablePlugin) Name() string {
	return PluginName
}

func (grp *GlobalRanktablePlugin) OnHyperJobAdd(ctx context.Context, hj *trainingv1alpha1.HyperJob) error {
	log := ctrl.LoggerFrom(ctx)

	for _, cmName := range configmapsNeedToCreate {
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
		job.Spec.Plugins[VCJobPluginName] = grp.args
	}

	for _, resource := range configmapsNeedToCreate {
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
