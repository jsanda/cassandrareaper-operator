/*


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

package controllers

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/k8ssandra/reaper-operator/pkg/status"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cassdcv1beta1 "github.com/datastax/cass-operator/operator/pkg/apis/cassandra/v1beta1"
	reapergo "github.com/k8ssandra/reaper-client-go/reaper"
	api "github.com/k8ssandra/reaper-operator/api/v1alpha1"
)

// CassandraDatacenterReconciler reconciles a CassandraDatacenter object
type CassandraDatacenterReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

const (
	DefaultStatusCheckDelay = 30 * time.Minute
	DefaultShortDelay       = 30 * time.Second
	DefaultLongDelay        = 10 * time.Minute
)

var (
	statusCheckDelay time.Duration
	shortDelay       time.Duration
	longDelay        time.Duration
)

func init() {
	statusCheckDelay = getReconcileDelay("REQUEUE_DELAY_STATUS_CHECK", DefaultStatusCheckDelay)
	shortDelay = getReconcileDelay("REQUEUE_DELAY_SHORT", DefaultShortDelay)
	longDelay = getReconcileDelay("REQUEUE_DELAY_LONG", DefaultLongDelay)
}

func getReconcileDelay(name string, defaultDelay time.Duration) time.Duration {
	value := os.Getenv(name)
	if len(value) == 0 {
		return defaultDelay
	} else {
		if delay, err := time.ParseDuration(value); err == nil {
			return delay
		} else {
			panic(fmt.Sprintf("failed to parse %s=%s", name, value))
		}
	}
}

// +kubebuilder:rbac:groups=cassandra.datastax.com,namespace="reaper-operator",resources=cassandradatacenters,verbs=get;list;watch;create

func (r *CassandraDatacenterReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	_ = r.Log.WithValues("cassandradatacenter", req.NamespacedName)
	statusManager := &status.StatusManager{Client: r.Client}

	instance := &cassdcv1beta1.CassandraDatacenter{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: shortDelay}, err
	}

	cassdc := instance.DeepCopy()

	if reaperName, ok := cassdc.Annotations["reaper.cassandra-reaper.io/instance"]; ok {
		reaperKey := getReaperKey(reaperName, cassdc.Namespace)
		reaperInstance := &api.Reaper{}

		err := r.Get(ctx, reaperKey, reaperInstance)
		if err != nil {
			if errors.IsNotFound(err) {
				// It is possible that the Reaper has not been deployed yet or that it has
				// been deleted, or the annotation could specify an incorrect value.
				r.Recorder.Event(reaperInstance, corev1.EventTypeNormal, "Find", fmt.Sprintf("No reaper instance: %v found", reaperKey))
				r.Log.Info("reaper instance not found", "reaper", reaperKey)
				return ctrl.Result{RequeueAfter: longDelay}, nil
			} else {
				r.Recorder.Event(reaperInstance, corev1.EventTypeWarning, "Find", fmt.Sprintf("Failed to retrieve reaper instance: %v", reaperKey))
				r.Log.Error(err, "failed to retrieve reaper instance", "reaper", reaperKey)
				return ctrl.Result{RequeueAfter: shortDelay}, err
			}
		}

		reaper := reaperInstance.DeepCopy()

		if !reaper.Status.Ready {
			r.Log.Info("waiting for reaper to become ready", "reaper", reaperKey)
			return ctrl.Result{RequeueAfter: shortDelay}, nil
		}

		// Include the namespace in case Reaper is deployed in a different namespace than
		// the CassandraDatacenter.
		reaperSvc := reaper.Name + "-reaper-service" + "." + reaper.Namespace
		restClient, err := reapergo.NewReaperClient(fmt.Sprintf("http://%s:8080", reaperSvc))
		if err != nil {
			r.Recorder.Event(reaper, corev1.EventTypeWarning, "Client", fmt.Sprintf("Failed to create client to access Reaper"))
			r.Log.Error(err, "failed to create reaper rest client", "reaperService", reaperSvc)
			return ctrl.Result{RequeueAfter: shortDelay}, err
		}

		_, err = restClient.GetCluster(ctx, cassdc.Spec.ClusterName)

		if err == nil {
			// The only thing left to do is to make sure that the cluster is listed in
			// Reaper's status. We still requeue the request to periodically check that
			// the cluster has not be removed from Reaper.
			if err = statusManager.AddClusterToStatus(ctx, reaper, cassdc); err == nil {
				r.Recorder.Event(reaper, corev1.EventTypeNormal, "Add", fmt.Sprintf("Added %s to cluster status", cassdc.ClusterName))
				return ctrl.Result{RequeueAfter: statusCheckDelay}, nil
			} else {
				r.Recorder.Event(reaper, corev1.EventTypeWarning, "Add", fmt.Sprintf("Failed to add %s to cluster status", cassdc.ClusterName))
				r.Log.Error(err, "failed to re-add cluster in reaper status", "reaper", reaperKey)
				return ctrl.Result{RequeueAfter: shortDelay}, err
			}
		}

		if err == reapergo.CassandraClusterNotFound {
			r.Log.Info("registering cluster with reaper", "reaper", reaperKey)
			if err = restClient.AddCluster(ctx, cassdc.Spec.ClusterName, cassdc.GetDatacenterServiceName()); err == nil {
				if err = statusManager.AddClusterToStatus(ctx, reaper, cassdc); err == nil {
					r.Recorder.Event(reaper, corev1.EventTypeNormal, "Add", fmt.Sprintf("Added %s to cluster status", cassdc.ClusterName))
					return ctrl.Result{RequeueAfter: statusCheckDelay}, nil
				} else {
					r.Recorder.Event(reaper, corev1.EventTypeWarning, "Add", fmt.Sprintf("Failed to add %s to cluster status", cassdc.ClusterName))
					r.Log.Error(err, "failed to add cluster in reaper status", "reaper", reaperKey)
					return ctrl.Result{RequeueAfter: shortDelay}, err
				}
			} else {
				r.Recorder.Event(reaper, corev1.EventTypeWarning, "Add", fmt.Sprintf("Failed to register cluster %s to reaper", cassdc.ClusterName))
				r.Log.Error(err, "failed to register cluster with reaper", "reaper", reaperKey)
				return ctrl.Result{RequeueAfter: shortDelay}, err
			}
		}
	}

	// The CassandraDatacenter does not have the annotation which means it is not using
	// Reaper to manage repairs. We requeue the request though to periodically check if
	// the cluster has been updated to be managed with Reaper.
	return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
}

func getReaperKey(instanceName, cassdcNamespace string) types.NamespacedName {
	parts := strings.Split(instanceName, ".")
	if len(parts) == 1 {
		return types.NamespacedName{Namespace: cassdcNamespace, Name: instanceName}
	} else {
		return types.NamespacedName{Namespace: parts[1], Name: parts[0]}
	}
}

func (r *CassandraDatacenterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cassdcv1beta1.CassandraDatacenter{}).
		Complete(r)
}
