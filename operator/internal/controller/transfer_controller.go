package controller

import (
	"context"
	"encoding/json"
	"fmt"

	trsfrcrdv1 "github.com/doublecloud/transfer/operator/api/v1"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TransferReconciler reconciles a Transfer object
type TransferReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=trsfrcrd.dc,resources=transfers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=trsfrcrd.dc,resources=transfers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=trsfrcrd.dc,resources=transfers/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the Transfer object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *TransferReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the CustomResource object (e.g., trsfrcrdv1.Transfer)
	transfer := &trsfrcrdv1.Transfer{}
	err := r.Get(ctx, req.NamespacedName, transfer)
	if err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	transfer.Namespace = req.NamespacedName.Namespace

	// Determine the transfer type (snapshot, regular_snapshot, replication, snapshot_replication)
	transferType := transfer.Spec.Type

	switch transferType {
	case abstract.TransferTypeSnapshotOnly:
		if transfer.Spec.RegularSnapshot != nil && transfer.Spec.RegularSnapshot.Enabled {
			return r.reconcileSnapshotCronJob(ctx, transfer, req.Name, transfer.Spec.RegularSnapshot.CronExpression)
		}
		return r.reconcileSnapshotCronJob(ctx, transfer, req.Name, "*/1 * * * *")
	case abstract.TransferTypeIncrementOnly:
		return r.reconcileReplication(ctx, transfer, req.Name)
	case abstract.TransferTypeSnapshotAndIncrement:
		return r.reconcileSnapshotReplication(ctx, transfer, req.NamespacedName)
	default:
		logger.Error(fmt.Errorf("unknown transfer type"), "Transfer type is unknown", "transferType", transferType)
		return reconcile.Result{}, nil
	}
}

// Reconcile snapshot: create a one-time CronJob
func (r *TransferReconciler) reconcileSnapshotCronJob(ctx context.Context, transfer *trsfrcrdv1.Transfer, name, cronSchedule string) (reconcile.Result, error) {
	transfer.Status.Status = model.Scheduled
	if err := r.Status().Update(ctx, transfer); err != nil {
		return reconcile.Result{}, err
	}

	configMapName := fmt.Sprintf("transfer-%s-config", name)

	// Step 1: Create the ConfigMap
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: transfer.Namespace,
		},
		Data: map[string]string{
			"config.yaml": createTransferConfig(transfer, name), // Generate config.yaml content
		},
	}

	// Ensure ConfigMap is created or updated
	result, err := r.ensureConfig(ctx, transfer, configMapName, configMap)
	if err != nil {
		return result, err
	}

	// Step 2: Create the one-time CronJob for snapshot
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      transfer.Name + "-snapshot",
			Namespace: transfer.Namespace,
		},
		Spec: batchv1.CronJobSpec{
			Schedule: cronSchedule, // Modify to one-time schedule as needed
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: r.buildContainer(transfer, configMapName, "activate", corev1.RestartPolicyNever, nil),
				},
			},
		},
	}

	// Ensure the CronJob is created
	existingCronJob := &batchv1.CronJob{}
	err = r.Get(ctx, client.ObjectKey{Name: cronJob.Name, Namespace: cronJob.Namespace}, existingCronJob)
	if err != nil && client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}
	if err != nil && errors.IsNotFound(err) {
		// Create new CronJob if it doesn't exist
		if err := controllerutil.SetControllerReference(transfer, cronJob, r.Scheme); err != nil {
			return reconcile.Result{}, err
		}
		err = r.Create(ctx, cronJob)
	} else {
		// Update CronJob if it exists
		existingCronJob.Spec = cronJob.Spec
		err = r.Update(ctx, existingCronJob)
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

// Reconcile replication: create a StatefulSet
func (r *TransferReconciler) reconcileReplication(ctx context.Context, transfer *trsfrcrdv1.Transfer, name string) (reconcile.Result, error) {
	configMapName := fmt.Sprintf("transfer-%s-config", name)

	// Step 1: Create the ConfigMap
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: transfer.Namespace,
		},
		Data: map[string]string{
			"config.yaml": createTransferConfig(transfer, name), // Generate config.yaml content
		},
	}

	// Ensure ConfigMap is created or updated
	result, err := r.ensureConfig(ctx, transfer, configMapName, configMap)
	if err != nil {
		return result, err
	}
	lbls := map[string]string{"app": transfer.Name + "-replication"}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      transfer.Name + "-replication",
			Namespace: transfer.Namespace,
			Labels:    lbls,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: transfer.Name + "-service",
			Replicas:    int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: lbls,
			},
			Template: r.buildContainer(transfer, configMapName, "activate", corev1.RestartPolicyAlways, lbls),
		},
	}

	// Ensure StatefulSet is created
	existingStatefulSet := &appsv1.StatefulSet{}
	err = r.Get(ctx, client.ObjectKey{Name: statefulSet.Name, Namespace: statefulSet.Namespace}, existingStatefulSet)
	if err != nil && client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}
	if err != nil && errors.IsNotFound(err) {
		// Create new StatefulSet if it doesn't exist
		if err := controllerutil.SetControllerReference(transfer, statefulSet, r.Scheme); err != nil {
			return reconcile.Result{}, err
		}
		err = r.Create(ctx, statefulSet)
	} else {
		// Update the existing StatefulSet
		existingStatefulSet.Spec = statefulSet.Spec
		err = r.Update(ctx, existingStatefulSet)
	}
	if err != nil {
		return reconcile.Result{}, err
	}
	transfer.Status.Status = model.Running
	if err := r.Status().Update(ctx, transfer); err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

// Reconcile snapshot and replication: first run snapshot, then deploy StatefulSet
func (r *TransferReconciler) reconcileSnapshotReplication(ctx context.Context, transfer *trsfrcrdv1.Transfer, name types.NamespacedName) (reconcile.Result, error) {
	// Step 1: Check if the snapshot has been executed
	if !transfer.Status.SnapshotComplete {
		transfer.Status.Status = model.Scheduled
		if err := r.Status().Update(ctx, transfer); err != nil {
			return reconcile.Result{}, err
		}
		// If not complete, run a one-time snapshot job
		result, err := r.reconcileSnapshotCronJob(ctx, transfer, name.Name, "*/1 * * * *")
		if err != nil || result.Requeue {
			return result, err
		}
		// Check if the CronJob has completed at least one job
		if completed, err := r.hasCompletedJobs(ctx, transfer, name.Namespace); err != nil {
			return reconcile.Result{}, err
		} else if completed {
			// Mark the snapshot as complete
			transfer.Status.SnapshotComplete = true
			transfer.Status.Status = model.Running
			err = r.Status().Update(ctx, transfer)
			if err != nil {
				return reconcile.Result{}, err
			}
		} else {
			// If there are no completed jobs, requeue to check again later
			return reconcile.Result{Requeue: true}, nil
		}
	}

	transfer.Namespace = name.Namespace
	// Step 2: Once snapshot is done, deploy StatefulSet for replication
	return r.reconcileReplication(ctx, transfer, name.Name)
}

// hasCompletedJobs checks if there are any completed jobs for the given transfer.
func (r *TransferReconciler) hasCompletedJobs(ctx context.Context, transfer *trsfrcrdv1.Transfer, namespace string) (bool, error) {
	// Create a list of jobs
	jobList := &batchv1.JobList{}
	// List jobs created by the CronJob
	err := r.List(ctx, jobList, client.InNamespace(namespace), client.MatchingLabels{"job-name": transfer.Name + "-snapshot"})
	if err != nil {
		return false, err
	}

	// Check if any job is completed successfully
	for _, job := range jobList.Items {
		if job.Status.Succeeded > 0 && job.Status.Failed == 0 {
			return true, nil // Found a completed job
		}
	}

	// No completed jobs found
	return false, nil
}

// Generates the content for the ConfigMap's config.yaml
func createTransferConfig(transfer *trsfrcrdv1.Transfer, name string) string {
	config := fmt.Sprintf(`
id: %s
type: %s
src:
  type: %s
  params: '%s'
dst:
  type: %s
  params: '%s'
`, name,
		transfer.Spec.Type,
		transfer.Spec.Src.Type,
		transfer.Spec.Src.Params,
		transfer.Spec.Dst.Type,
		transfer.Spec.Dst.Params)

	if transfer.Spec.RegularSnapshot != nil {
		config += fmt.Sprintf("regular_snapshot: %s\n", toJSON(transfer.Spec.RegularSnapshot))
	}
	if transfer.Spec.DataObjects != nil {
		config += fmt.Sprintf("data_objects: %s\n", toJSON(transfer.Spec.DataObjects))
	}
	if transfer.Spec.Transformation != "" {
		config += fmt.Sprintf("transformation: %s\n", toJSON(transfer.Spec.Transformation))
	}

	return config
}

// Convert Go struct to JSON string
func toJSON(data interface{}) string {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "{}"
	}
	return string(jsonData)
}

func (r *TransferReconciler) buildContainer(
	transfer *trsfrcrdv1.Transfer,
	configMapName, command string,
	restartPolicy corev1.RestartPolicy,
	lbls map[string]string,
) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: lbls,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "transfer",
					Image:   transfer.Config.Image,
					Command: generateCommand(transfer, command),
					Env:     generateEnvVars(transfer.Spec.Env),
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "config-volume",
							MountPath: "/var/config/config.yaml",
							SubPath:   "config.yaml",
						},
					},
					Resources: transfer.Resources,
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "config-volume",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: configMapName,
							},
							Items: []corev1.KeyToPath{
								{
									Key:  "config.yaml",
									Path: "config.yaml",
								},
							},
						},
					},
				},
			},
			RestartPolicy: restartPolicy,
		},
	}
}

func (r *TransferReconciler) ensureConfig(
	ctx context.Context,
	transfer *trsfrcrdv1.Transfer,
	configMapName string,
	configMap *corev1.ConfigMap,
) (reconcile.Result, error) {
	existingConfigMap := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKey{Name: configMapName, Namespace: transfer.Namespace}, existingConfigMap)
	if err != nil && errors.IsNotFound(err) {
		// If the ConfigMap doesn't exist, create it
		if err := controllerutil.SetControllerReference(transfer, configMap, r.Scheme); err != nil {
			return reconcile.Result{}, err
		}
		err = r.Create(ctx, configMap)
		if err != nil {
			return reconcile.Result{}, err
		}
	} else if err == nil {
		// If the ConfigMap exists, update it
		existingConfigMap.Data = configMap.Data
		err = r.Update(ctx, existingConfigMap)
		if err != nil {
			return reconcile.Result{}, nil
		}
	} else {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, err
}

// Generate the container command based on transfer Spec
func generateCommand(transfer *trsfrcrdv1.Transfer, commang string) []string {
	return []string{
		"/usr/local/bin/trcli",
		commang,
		"--transfer",
		"/var/config/config.yaml",
		"--coordinator",
		transfer.Config.Coordinator.Type,
		"--coordinator-s3-bucket",
		transfer.Config.Coordinator.S3Bucket,
	}
}

// Generate environment variables for the container
func generateEnvVars(envVars map[string]string) []corev1.EnvVar {
	var envs []corev1.EnvVar
	for name, value := range envVars {
		envs = append(envs, corev1.EnvVar{Name: name, Value: value})
	}
	return envs
}

// Helper function to get pointer of int32
func int32Ptr(i int32) *int32 {
	return &i
}

// SetupWithManager sets up the controller with the Manager.
func (r *TransferReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&trsfrcrdv1.Transfer{}).
		Complete(r)
}
