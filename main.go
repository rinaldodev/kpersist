package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nsf/jsondiff"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// TODO logs, structure, tests, error handling, etc

const (
	BasePath  = "./kpersist-logs"
	K8sEnvVar = "KUBECONFIG"
)

func main() {
	c, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	// TODO allow other ways to point to k8s config
	kubeconfig := os.Getenv(K8sEnvVar)

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		slog.ErrorContext(c, "Error getting k8s config from path", "error", err, "kubeconfig path", kubeconfig)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.ErrorContext(c, "Error creating k8s clientset for config", "error", err, "config", config)
		os.Exit(1)
	}

	//forConfig, err := discovery.NewDiscoveryClientForConfig(config)
	//if err != nil {
	//	return
	//}
	//forConfig.

	dclient, err := dynamic.NewForConfig(config)
	if err != nil {
		slog.ErrorContext(c, "Error creating k8s dynamic client for config", "error", err, "config", config)
		os.Exit(1)
	}

	path := filepath.Join(BasePath, time.Now().Format("20060102_150405.00000"))
	slog.InfoContext(c, "Creating directory for storing files", "path", path)

	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		slog.ErrorContext(c, "Error creating directory for storing files", "error", err, "path", path)
		os.Exit(1)
	}

	slog.InfoContext(c, "Initializing resource watchers")
	go watchResources(c, path, dclient)

	slog.InfoContext(c, "Initializing logs persisters")
	go watchPodsForLogs(c, path, clientset)

	<-c.Done()
}

func watchResources(c context.Context, path string, dclient *dynamic.DynamicClient) {
	resource := schema.GroupVersionResource{
		Group:    "camel.apache.org",
		Version:  "v1",
		Resource: "integrations",
	}

	watcher, err := dclient.
		Resource(resource).
		Watch(c, metav1.ListOptions{})
	if err != nil {
		slog.ErrorContext(c, "Failed to create watcher for resource", "error", err, "resource", resource)
	}

	channels := map[string]chan *unstructured.Unstructured{}

	for event := range watcher.ResultChan() {
		switch event.Type {
		case watch.Added:
			fmt.Printf("Watch Integration Event: %s\n", event.Type)

			typedObj := event.Object.(*unstructured.Unstructured)

			name := typedObj.GetName()
			ch := make(chan *unstructured.Unstructured, 10)

			time := time.Now().Format("20060102_150405.00000")
			fileName := filepath.Join(path, fmt.Sprintf("%s_integration_%s.txt", time, name))
			fmt.Printf("Writing to file %s\n", fileName)

			file := writeInitialFileContent(fileName, typedObj)

			channels[name] = ch
			go processResourceModifications(file, ch, typedObj)
		case watch.Modified:
			fmt.Printf("Watch Integration Event: %s\n", event.Type)
			typedObj := event.Object.(*unstructured.Unstructured)
			name := typedObj.GetName()
			channels[name] <- typedObj
		case watch.Deleted:
			typedObj := event.Object.(*unstructured.Unstructured)
			name := typedObj.GetName()
			close(channels[name])
			delete(channels, name)
		default:
			fmt.Printf("Unsupported Integration Event: %s\n", event.Type)
		}
	}
}

func writeInitialFileContent(fileName string, typedObj *unstructured.Unstructured) *os.File {
	out, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("Error while creating file %s: %s\n", fileName, err.Error())
		panic(err)
	}

	typedObj.SetManagedFields(nil)
	jsonBytes, err := typedObj.MarshalJSON()
	if err != nil {
		panic(err)
	}

	var prettyJson bytes.Buffer
	err = json.Indent(&prettyJson, jsonBytes, "", "\t")
	if err != nil {
		panic(err)
	}

	_, err = out.Write(prettyJson.Bytes())
	if err != nil {
		fmt.Printf("Error while writing resource to %s: %s\n", fileName, err.Error())
		_, err := out.WriteString(fmt.Sprintf("kpersist: error occurred while writing this log file: %s", err.Error()))
		if err != nil {
			panic(err)
		}
		panic(err)
	}

	return out
}

func processResourceModifications(out *os.File, ch chan *unstructured.Unstructured, obj *unstructured.Unstructured) {
	prev := obj
	prev.SetManagedFields(nil)

	defer out.Close()

	for curr := range ch {
		prevJson, err := prev.MarshalJSON()
		if err != nil {
			panic(err)
		}

		curr.SetManagedFields(nil)

		currJson, err := curr.MarshalJSON()
		if err != nil {
			panic(err)
		}

		options := jsondiff.DefaultJSONOptions()
		options.SkipMatches = true
		_, diffs := jsondiff.Compare(prevJson, currJson, &options)

		_, err = out.WriteString(fmt.Sprintf("\n----------------------------------\nChange captured at %s:\n", time.Now().Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		_, err = out.WriteString(fmt.Sprintf("%s\n----------------------------------\n", diffs))
		if err != nil {
			fmt.Printf("Error while writing resource to %s: %s\n", out.Name(), err.Error())
			_, err := out.WriteString(fmt.Sprintf("kpersist: error occurred while writing this log file: %s", err.Error()))
			if err != nil {
				panic(err)
			}
			panic(err)
		}

		err = out.Sync()
		if err != nil {
			panic(err)
		}

		prev = curr
	}
}

type watchedPod struct {
	cond     *sync.Cond
	pod      *corev1.Pod
	canRetry bool
}

func watchPodsForLogs(ctx context.Context, basePath string, clientset *kubernetes.Clientset) {
	// TODO allow setting selectors
	watcher, err := clientset.CoreV1().Pods("").Watch(ctx, metav1.ListOptions{LabelSelector: "camel.apache.org/integration"})
	if err != nil {
		panic(err)
	}

	pods := map[types.UID]*watchedPod{}

	for event := range watcher.ResultChan() {
		fmt.Printf("Watch Pod Event: %s\n", event.Type)
		if event.Type == watch.Added {
			pod := event.Object.(*corev1.Pod)

			wp := watchedPod{
				pod:      pod,
				cond:     sync.NewCond(&sync.Mutex{}),
				canRetry: true,
			}

			pods[pod.UID] = &wp

			go followAndPersistContainerLog(ctx, basePath, wp)
		}
		if event.Type == watch.Modified {
			pod := event.Object.(*corev1.Pod)
			wp := pods[pod.UID]
			if wp != nil {
				wp.cond.Signal()
			} else {
				fmt.Printf("Pod modified that wasn't previously known: %s/%s", pod.Name, pod.UID)
			}
		}
		if event.Type == watch.Deleted {
			pod := event.Object.(*corev1.Pod)
			wp := pods[pod.UID]
			if wp != nil {
				wp.cond.L.Lock()
				wp.canRetry = false
				wp.cond.L.Unlock()
				wp.cond.Signal()
				delete(pods, pod.UID)
			} else {
				fmt.Printf("Pod deleted that wasn't previously known: %s/%s", pod.Name, pod.UID)
			}
		}
	}
}

func followAndPersistContainerLog(c context.Context, basePath string, wp watchedPod) {

	time := time.Now().Format("20060102_150405.00000")
	fileName := filepath.Join(basePath, fmt.Sprintf("%s_pod_%s.txt", time, wp.pod.Name))
	fmt.Printf("Writing to file %s\n", fileName)

	out, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("Error while creating file %s: %s\n", fileName, err.Error())
		panic(err)
	}
	defer out.Close()
	defer out.Sync()

	wp.cond.L.Lock()
	defer wp.cond.L.Unlock()
	for cont := true; cont; cont = wp.canRetry {
		execKubectl(c, wp, out)
		wp.cond.Wait()
	}

	s := fmt.Sprintf("Done writing logs from pod %s", wp.pod.Name)
	fmt.Println(s)
	out.WriteString(s)
}

func execKubectl(c context.Context, wp watchedPod, out *os.File) {
	kubectl := exec.CommandContext(
		c,
		"kubectl",
		"logs",
		wp.pod.Name,
		"-n="+wp.pod.Namespace,
		"--follow=true",
		"--all-containers=true",
		"--ignore-errors=true",
		"--pod-running-timeout=5m",
		"--prefix=true",
		"--timestamps=true",
	)

	// sed to remove colors
	sed := exec.CommandContext(
		c,
		"sed",
		"s/\\x1B\\[[0-9;]\\{1,\\}[A-Za-z]//g",
	)

	// pipe kubectl output to sed input
	var err error
	sed.Stdin, err = kubectl.StdoutPipe()
	if err != nil {
		fmt.Printf("Error getting logs: %s\n", err.Error())
		panic(err)
	}

	// write sed and all errors to file (out)
	kubectl.Stderr = out
	sed.Stdout = out
	sed.Stderr = out

	err = sed.Start()
	if err != nil {
		s := fmt.Sprintf("Error when trying to get logs from pod %s: %+v", wp.pod.Name, err)
		fmt.Println(s)
		out.WriteString(s)
		return
	}

	defer func() {
		err = sed.Wait()
		if err != nil {
			s := fmt.Sprintf("Error when trying to get logs from pod %s: %+v", wp.pod.Name, err)
			fmt.Println(s)
			out.WriteString(s)
		}
	}()

	err = kubectl.Run()
	if err != nil {
		s := fmt.Sprintf("Error when trying to get logs from pod %s: %+v", wp.pod.Name, err)
		fmt.Println(s)
		out.WriteString(s)
		return
	}
}
