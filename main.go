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
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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

	forConfig, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return
	}
	forConfig.

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

			fileName := filepath.Join(path, fmt.Sprintf("integration_%s.txt", name))
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

	defer out.Close()

	for curr := range ch {
		prevJson, err := prev.MarshalJSON()
		if err != nil {
			panic(err)
		}

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

func watchPodsForLogs(ctx context.Context, basePath string, clientset *kubernetes.Clientset) {
	// TODO allow setting selectors
	watcher, err := clientset.CoreV1().Pods("").Watch(ctx, metav1.ListOptions{LabelSelector: "camel.apache.org/integration"})
	if err != nil {
		panic(err)
	}

	for event := range watcher.ResultChan() {
		fmt.Printf("Watch Pod Event: %s\n", event.Type)
		if event.Type == watch.Added {
			pod := event.Object.(*corev1.Pod)
			//for _, container := range pod.Spec.Containers {
			logRequest := clientset.CoreV1().
				Pods(pod.Namespace).
				GetLogs(
					pod.Name,
					&corev1.PodLogOptions{
						Follow: true,
						//				Container: container.Name,
					},
				)
			//go followAndPersistContainerLog(ctx, basePath, pod, container, logRequest)
			go followAndPersistContainerLog(ctx, basePath, pod, logRequest)
			//}
		}
	}
}

func followAndPersistContainerLog(c context.Context, path string, pod *corev1.Pod, logRequest *rest.Request) {

	fileName := filepath.Join(path, fmt.Sprintf("pod_%s.txt", pod.Name))
	fmt.Printf("Writing to file %s\n", fileName)

	out, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("Error while creating file %s: %s\n", fileName, err.Error())
		panic(err)
	}
	defer out.Close()
	defer out.Sync()

	cmd := exec.CommandContext(
		c,
		"kubectl",
		"logs",
		pod.Name,
		"--follow=true",
		"--all-containers=true",
		"--ignore-errors=true",
		"--pod-running-timeout=5m",
		"--prefix=true",
		//"--previous=true",
		"--timestamps=true",
	)

	cmd.Stdout = out
	cmd.Stderr = out

	err = cmd.Run()
	if err != nil {
		s := fmt.Sprintf("Error when trying to get logs from pod %s: %+v", pod.Name, err)
		fmt.Println(s)
		out.WriteString(s)
		return
	}

	s := fmt.Sprintf("Done writing logs from pod %s", pod.Name)
	fmt.Println(s)
	out.WriteString(s)
}
