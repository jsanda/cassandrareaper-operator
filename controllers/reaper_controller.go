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
	"k8s.io/client-go/tools/record"
	"time"

	"github.com/go-logr/logr"
	"github.com/k8ssandra/reaper-operator/pkg/config"
	"github.com/k8ssandra/reaper-operator/pkg/status"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/k8ssandra/reaper-operator/api/v1alpha1"
	"github.com/k8ssandra/reaper-operator/pkg/reconcile"
	appsv1 "k8s.io/api/apps/v1"
)

// ReaperReconciler reconciles a Reaper object
type ReaperReconciler struct {
	client.Client
	Log                  logr.Logger
	Scheme               *runtime.Scheme
	ServiceReconciler    reconcile.ServiceReconciler
	DeploymentReconciler reconcile.DeploymentReconciler
	SchemaReconciler     reconcile.SchemaReconciler
	Validator            config.Validator
	Recorder             record.EventRecorder
}

// +kubebuilder:rbac:groups=reaper.cassandra-reaper.io,namespace="reaper-operator",resources=reapers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=reaper.cassandra-reaper.io,namespace="reaper-operator",resources=reapers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="apps",namespace="reaper-operator",resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="batch",namespace="reaper-operator",resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",namespace="reaper-operator",resources=services,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",namespace="reaper-operator",resources=secrets,verbs=get;list;watch

func (r *ReaperReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	reqLogger := r.Log.WithValues("reaper", req.NamespacedName)
	statusManager := &status.StatusManager{Client: r.Client}

	reqLogger.Info("starting reconciliation")

	// Fetch the Reaper instance
	instance := &api.Reaper{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, err
	}

	instance = instance.DeepCopy()

	if err := r.Validator.Validate(instance); err != nil {
		return ctrl.Result{}, err
	}

	if r.Validator.SetDefaults(instance) {
		if err = r.Update(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	reaperReq := reconcile.ReaperRequest{Reaper: instance, Logger: reqLogger, StatusManager: statusManager, Recorder: r.Recorder}

	if result, err := r.ServiceReconciler.ReconcileService(ctx, reaperReq); result != nil {
		return *result, err
	}

	if result, err := r.SchemaReconciler.ReconcileSchema(ctx, reaperReq); result != nil {
		return *result, err
	}

	if result, err := r.DeploymentReconciler.ReconcileDeployment(ctx, reaperReq); result != nil {
		return *result, err
	}

	reqLogger.Info("the reaper instance is reconciled")

	return ctrl.Result{}, nil
}

func (r *ReaperReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&api.Reaper{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}
