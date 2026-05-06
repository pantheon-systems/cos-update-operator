package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/go-systemd/login1"
	"github.com/golang/glog"
	"github.com/pkg/errors"

	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	v1meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"

	kwatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/watch"

	"github.com/pantheon-systems/cos-update-operator/pkg/constants"
	"github.com/pantheon-systems/cos-update-operator/pkg/drain"
	"github.com/pantheon-systems/cos-update-operator/pkg/k8sutil"
	"github.com/pantheon-systems/cos-update-operator/pkg/updateengine"
)

type Klocksmith struct {
	node            string
	kc              kubernetes.Interface
	nc              v1core.NodeInterface
	ue              *updateengine.Client
	lc              *login1.Conn
	reapTimeout     time.Duration
	drainPods       bool
	waitForPodLabel string
}

const defaultPollInterval = 10 * time.Second

var (
	shouldRebootSelector = fields.Set(map[string]string{
		constants.AnnotationOkToReboot:   constants.True,
		constants.AnnotationRebootNeeded: constants.True,
	}).AsSelector()
)

func New(node string, reapTimeout time.Duration, drainPods bool, waitForPodLabel string) (*Klocksmith, error) {
	// set up kubernetes in-cluster client
	kc, err := k8sutil.GetClient()
	if err != nil {
		return nil, fmt.Errorf("error creating kubernetes client: %v", err)
	}

	// node interface
	nc := kc.CoreV1().Nodes()

	// set up update_engine client
	ue, err := updateengine.New()
	if err != nil {
		return nil, fmt.Errorf("error establishing connection to update_engine dbus: %v", err)
	}

	// set up login1 client for our eventual reboot
	lc, err := login1.New()
	if err != nil {
		return nil, fmt.Errorf("error establishing connection to logind dbus: %v", err)
	}

	return &Klocksmith{node, kc, nc, ue, lc, reapTimeout, drainPods, waitForPodLabel}, nil
}

// Run starts the agent to listen for an update_engine reboot signal and react
// by draining pods and rebooting. Runs until the stop channel is closed.
func (k *Klocksmith) Run(stop <-chan struct{}) {
	glog.V(5).Info("Starting agent")

	// agent process should reboot the node, no need to loop
	if err := k.process(stop); err != nil {
		glog.Errorf("Error running agent process: %v", err)
	}

	glog.V(5).Info("Stopping agent")
}

// process performs the agent reconciliation to reboot the node or stops when
// the stop channel is closed.
func (k *Klocksmith) process(stop <-chan struct{}) error {
	if err := k.waitForNodeReady(); err != nil {
		return errors.Wrap(err, "failed to wait for node ready")
	}

	if err := k.waitForPodWithLabel(); err != nil {
		return errors.Wrap(err, "failed to wait for pod with label")
	}

	glog.Info("Setting info labels")
	if err := k.setInfoLabels(); err != nil {
		return fmt.Errorf("failed to set node info: %v", err)
	}

	glog.Info("Checking annotations")
	node, err := k.nc.Get(context.TODO(), k.node, v1meta.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node %q: %v", node, err)
	}
	if node == nil {
		return errors.New("node is nil but node client returned no error")
	}

	// Only make a node schedulable if a reboot was in progress. This prevents a node from being made schedulable
	// if it was made unschedulable by something other than the agent
	madeUnschedulableAnnotation, madeUnschedulableAnnotationExists := node.Annotations[constants.AnnotationAgentMadeUnschedulable]
	makeSchedulable := madeUnschedulableAnnotation == constants.True

	// set coreos.com/update1/reboot-in-progress=false and
	// coreos.com/update1/reboot-needed=false
	anno := map[string]string{
		constants.AnnotationRebootInProgress: constants.False,
		constants.AnnotationRebootNeeded:     constants.False,
	}
	labels := map[string]string{
		constants.LabelRebootNeeded: constants.False,
	}
	glog.Infof("Setting annotations %#v", anno)
	if err := k8sutil.SetNodeAnnotationsLabels(k.nc, k.node, anno, labels); err != nil {
		return err
	}

	// Since we set 'reboot-needed=false', 'ok-to-reboot' should clear.
	// Wait for it to do so, else we might start reboot-looping
	if err := k.waitForNotOkToReboot(); err != nil {
		return err
	}

	if makeSchedulable {
		// we are schedulable now.
		glog.Info("Marking node as schedulable")
		if err := k8sutil.Unschedulable(k.nc, k.node, false); err != nil {
			return err
		}

		anno = map[string]string{
			constants.AnnotationAgentMadeUnschedulable: constants.False,
		}

		glog.Infof("Setting annotations %#v", anno)
		if err := k8sutil.SetNodeAnnotations(k.nc, k.node, anno); err != nil {
			return err
		}
	} else if madeUnschedulableAnnotationExists { // Annotation exists so node was marked unschedulable by external source
		glog.Info("Skipping marking node as schedulable -- node was marked unschedulable by an external source")
	}

	// watch update engine for status updates
	go k.watchUpdateStatus(k.updateStatusCallback, stop)

	// block until constants.AnnotationOkToReboot is set
	for {
		glog.Infof("Waiting for ok-to-reboot from controller...")
		err := k.waitForOkToReboot()
		if err == nil {
			// time to reboot
			break
		}
		glog.Warningf("error waiting for an ok-to-reboot: %v", err)
	}

	glog.Info("Checking if node is already unschedulable")
	node, err = k8sutil.GetNodeRetry(k.nc, k.node)
	if err != nil {
		return err
	}
	alreadyUnschedulable := node.Spec.Unschedulable

	// set constants.AnnotationRebootInProgress and drain self
	anno = map[string]string{
		constants.AnnotationRebootInProgress: constants.True,
	}

	if !alreadyUnschedulable {
		anno[constants.AnnotationAgentMadeUnschedulable] = constants.True
	}

	glog.Infof("Setting annotations %#v", anno)
	if err := k8sutil.SetNodeAnnotations(k.nc, k.node, anno); err != nil {
		return err
	}

	// drain self equates to:
	// 1. set Unschedulable if necessary
	// 2. delete all pods
	// unlike `kubectl drain`, we do not care about emptyDir or orphan pods
	// ('any pods that are neither mirror pods nor managed by
	// ReplicationController, ReplicaSet, DaemonSet or Job')

	if !alreadyUnschedulable {
		glog.Info("Marking node as unschedulable")
		if err := k8sutil.Unschedulable(k.nc, k.node, true); err != nil {
			return err
		}
	} else {
		glog.Info("Node already marked as unschedulable")
	}

	glog.Info("Getting pod list for deletion")
	pods, err := k.getPodsForDeletion()
	if err != nil {
		return err
	}

	// delete the pods.
	// TODO(mischief): explicitly don't terminate self? we'll probably just be a
	// mirror pod or daemonset anyway..
	glog.Infof("Deleting %d pods", len(pods))
	deleteOptions := &v1meta.DeleteOptions{}
	for _, pod := range pods {
		glog.Infof("Terminating pod %q...", pod.Name)
		if err := k.kc.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, *deleteOptions); err != nil {
			glog.Errorf("failed terminating pod %q: %v", pod.Name, err)
			// Continue anyways, the reboot should terminate it
		}
	}

	// wait for the pods to delete completely.
	wg := sync.WaitGroup{}
	for _, pod := range pods {
		wg.Add(1)
		go func(pod v1.Pod) {
			glog.Infof("Waiting for pod %q to terminate", pod.Name)
			if err := k.waitForPodDeletion(pod); err != nil {
				glog.Errorf("Skipping wait on pod %q: %v", pod.Name, err)
			}
			wg.Done()
		}(pod)
	}
	wg.Wait()

	glog.Info("Node drained, rebooting")

	// reboot
	k.lc.Reboot(false)

	// cross fingers
	sleepOrDone(24*7*time.Hour, stop)
	return nil
}

// updateStatusCallback receives Status messages from update engine. If the
// status is UpdateStatusUpdatedNeedReboot, indicate that with a label on our
// node.
func (k *Klocksmith) updateStatusCallback(s *updateengine.StatusResult) {
	glog.Info("Updating status")
	// update our status
	anno := map[string]string{
		constants.AnnotationStatus:          fmt.Sprintf("UPDATE_STATUS_%s", updateengine.Operation_name[int32(s.CurrentOperation)]),
		constants.AnnotationLastCheckedTime: fmt.Sprintf("%d", s.LastCheckedTime),
		constants.AnnotationNewVersion:      s.NewVersion,
	}

	labels := map[string]string{}

	// indicate we need a reboot
	if s.CurrentOperation == updateengine.Operation_UPDATED_NEED_REBOOT {
		glog.Info("Indicating a reboot is needed")
		anno[constants.AnnotationRebootNeeded] = constants.True
		labels[constants.LabelRebootNeeded] = constants.True
	}

	err := wait.PollUntilContextCancel(context.Background(), defaultPollInterval, true, func(ctx context.Context) (bool, error) {
		if ierr := k8sutil.SetNodeAnnotationsLabels(k.nc, k.node, anno, labels); ierr != nil {
			glog.Errorf("Failed to set annotation %q: %v", constants.AnnotationStatus, ierr)
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		glog.Errorf("polling until set annotations and labels on update status failed: %v", err)
	}
}

// setInfoLabels labels our node with helpful info about Container Linux.
// TODO: port this from coreos to cos
func (k *Klocksmith) setInfoLabels() error {
	// vi, err := k8sutil.GetVersionInfo()
	// if err != nil {
	// 	return fmt.Errorf("failed to get version info: %v", err)
	// }

	// labels := map[string]string{
	// 	constants.LabelID:      vi.ID,
	// 	constants.LabelGroup:   vi.Group,
	// 	constants.LabelVersion: vi.Version,
	// }

	// if err := k8sutil.SetNodeLabels(k.nc, k.node, labels); err != nil {
	// 	return err
	// }

	return nil
}

func (k *Klocksmith) watchUpdateStatus(update func(s *updateengine.StatusResult), stop <-chan struct{}) {
	glog.Info("Beginning to watch update_engine status")

	oldOperation := updateengine.Operation_IDLE
	ch := make(chan *updateengine.StatusResult, 1)

	go k.ue.ReceiveStatuses(ch, stop)

	for status := range ch {
		glog.Infof("status: %+v", status)
		if status.CurrentOperation != oldOperation && update != nil {
			update(status)
			oldOperation = status.CurrentOperation
		}
	}
}

// waitForOkToReboot waits for both 'ok-to-reboot' and 'needs-reboot' to be true.
func (k *Klocksmith) waitForOkToReboot() error {
	n, err := k.nc.Get(context.TODO(), k.node, v1meta.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get self node (%q): %v", k.node, err)
	}

	if n.Annotations[constants.AnnotationOkToReboot] == constants.True &&
		n.Annotations[constants.AnnotationRebootNeeded] == constants.True {
		return nil
	}
	lw := cache.NewListWatchFromClient(
		k.kc.CoreV1().RESTClient(),
		"nodes",
		n.Namespace,
		fields.OneTermEqualSelector("metadata.name", n.Name),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Hour*24))
	defer cancel()
	// hopefully 24 hours is enough time between indicating we need a
	// reboot and the controller telling us to do it
	ev, err := watch.UntilWithSync(ctx, lw, &v1.Node{}, nil, k8sutil.NodeAnnotationCondition(shouldRebootSelector))
	if err != nil {
		return fmt.Errorf("waiting for annotation %q failed: %v", constants.AnnotationOkToReboot, err)
	}

	// sanity check
	no, ok := ev.Object.(*v1.Node)
	if !ok {
		panic("event contains a non-*api.Node object")
	}

	if no.Annotations[constants.AnnotationOkToReboot] != constants.True {
		panic("event did not contain annotation expected")
	}

	return nil
}

func (k *Klocksmith) waitForNotOkToReboot() error {
	n, err := k.nc.Get(context.TODO(), k.node, v1meta.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get self node (%q): %v", k.node, err)
	}

	if n.Annotations[constants.AnnotationOkToReboot] != constants.True {
		return nil
	}

	lw := cache.NewListWatchFromClient(
		k.kc.CoreV1().RESTClient(),
		"nodes",
		n.Namespace,
		fields.OneTermEqualSelector("metadata.name", n.Name),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Hour*24))
	defer cancel()

	// Within 24 hours of indicating we don't need a reboot we should be given a not-ok.
	// If that isn't the case, it likely means the operator isn't running, and
	// we'll just crash-loop in that case, and hopefully that will help the user realize something's wrong.
	// Use a custom condition function to use the more correct 'OkToReboot !=
	// true' vs '== False'; due to the operator matching on '== True', and not
	// going out of its way to convert '' => 'False', checking the exact inverse
	// of what the operator checks is the correct thing to do.
	condition := func(event kwatch.Event) (bool, error) {
		switch event.Type {
		case kwatch.Error:
			return false, fmt.Errorf("error watching node: %v", event.Object)
		case kwatch.Deleted:
			return false,
				fmt.Errorf("node deleted while waiting for ready: %q", n.Name)
		}

		no := event.Object.(*v1.Node)
		if no.Annotations[constants.AnnotationOkToReboot] != constants.True {
			return true, nil
		}
		return false, nil
	}
	ev, err := watch.UntilWithSync(ctx, lw, &v1.Node{}, nil, watch.ConditionFunc(condition))
	if err != nil {
		return fmt.Errorf(
			"waiting for annotation %q failed: %v",
			constants.AnnotationOkToReboot,
			err,
		)
	}

	// sanity check
	no := ev.Object.(*v1.Node)

	if no.Annotations[constants.AnnotationOkToReboot] == constants.True {
		panic("event did not contain annotation expected")
	}

	return nil
}

func (k *Klocksmith) getPodsForDeletion() ([]v1.Pod, error) {
	pods, err := drain.GetPodsForDeletion(k.kc, k.node)
	if err != nil {
		return nil, fmt.Errorf("failed to get list of pods for deletion: %v", err)
	}

	// XXX: ignoring kube-system is a simple way to avoid evicting
	// critical components such as kube-scheduler and
	// kube-controller-manager.

	pods = k8sutil.FilterPods(pods, func(p *v1.Pod) bool {
		return k.drainPods && p.Namespace != "kube-system"
	})

	return pods, nil
}

// waitForPodDeletion waits for a pod to be deleted
func (k *Klocksmith) waitForPodDeletion(pod v1.Pod) error {
	ctx, cancel := context.WithTimeout(context.Background(), k.reapTimeout)
	defer cancel()
	return wait.PollUntilContextTimeout(ctx, defaultPollInterval, k.reapTimeout, true, func(ctx context.Context) (bool, error) {
		p, err := k.kc.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, v1meta.GetOptions{})
		if k8serrors.IsNotFound(err) || (p != nil && p.ObjectMeta.UID != pod.ObjectMeta.UID) {
			glog.Infof("Deleted pod %q", pod.Name)
			return true, nil
		}

		// most errors will be transient. log the error and continue
		// polling
		if err != nil {
			glog.Errorf("Failed to get pod %q: %v", pod.Name, err)
		}

		return false, nil
	})
}

// wait until a pod matching the provided label is running on this node
func (k *Klocksmith) waitForPodWithLabel() error {
	if k.waitForPodLabel == "" {
		// No label specified, nothing to do
		return nil
	}

	for {
		glog.Infof("Waiting for pod running on this node matching label: %s", k.waitForPodLabel)
		time.Sleep(defaultPollInterval)

		podList, err := k.kc.CoreV1().Pods(v1.NamespaceAll).List(context.TODO(), v1meta.ListOptions{
			FieldSelector: fields.SelectorFromSet(fields.Set{"spec.nodeName": k.node}).String(),
			LabelSelector: k.waitForPodLabel,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to get pods matching label: %s", k.waitForPodLabel)
		}

		for _, pod := range podList.Items {
			if pod.Status.Phase == v1.PodRunning {
				glog.Infof("Done waiting, found pod in running state: %s", pod.Name)
				return nil
			}
		}
	}
}

func (k *Klocksmith) waitForNodeReady() error {
	for {
		glog.Info("Waiting for node to be ready")
		time.Sleep(defaultPollInterval)

		node, err := k.kc.CoreV1().Nodes().Get(context.TODO(), k.node, v1meta.GetOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to get node: %s", k.node)
		}

		for _, cond := range node.Status.Conditions {
			if cond.Type == v1.NodeReady && cond.Status == v1.ConditionTrue {
				glog.Info("Done waiting, condition NodeReady is true")
				return nil
			}
		}
	}
}

// sleepOrDone pauses the current goroutine until the done channel receives
// or until at least the duration d has elapsed, whichever comes first. This
// is similar to time.Sleep(d), except it can be interrupted.
func sleepOrDone(d time.Duration, done <-chan struct{}) {
	sleep := time.NewTimer(d)
	defer sleep.Stop()
	select {
	case <-sleep.C:
		return
	case <-done:
		return
	}
}
