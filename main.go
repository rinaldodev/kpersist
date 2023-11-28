package main

import (
	"context"
	"fmt"
	"github.com/nsf/jsondiff"
	"io"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
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
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	dclient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	path := filepath.Join(BasePath, time.Now().Format("20060102_150405.00000"))
	fmt.Printf("Creating directory for storing files: %s\n", path)
	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		panic(err)
	}

	go watchResources(c, path, dclient)
	go watchPodsForLogs(c, path, clientset)

	<-c.Done()
}

func watchResources(c context.Context, path string, dclient *dynamic.DynamicClient) {
	watcher, err := dclient.
		// TODO generalize to any kind of resource
		Resource(
			schema.GroupVersionResource{
				Group:    "camel.apache.org",
				Version:  "v1",
				Resource: "integrations",
			}).
		Watch(c, metav1.ListOptions{})
	if err != nil {
		panic(err)
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

	bytes, err := typedObj.MarshalJSON()
	if err != nil {
		panic(err)
	}

	_, err = out.Write(bytes)
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

		_, err = out.WriteString(fmt.Sprintf("----------------------------------\nChange captured at %s:\n", time.Now().Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		_, err = out.WriteString(fmt.Sprintf("%s\n----------------------------------", diffs))
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

func watchPodsForLogs(c context.Context, path string, clientset *kubernetes.Clientset) {
	// TODO allow setting selectors
	watcher, err := clientset.CoreV1().Pods("").Watch(c, metav1.ListOptions{LabelSelector: "camel.apache.org/integration"})
	if err != nil {
		panic(err)
	}

	for event := range watcher.ResultChan() {
		fmt.Printf("Watch Pod Event: %s\n", event.Type)
		if event.Type == watch.Added {
			pod := event.Object.(*v1.Pod)
			req := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &v1.PodLogOptions{Follow: true})
			go followAndPersistPodLog(c, path, pod, req)
		}
	}
}

func followAndPersistPodLog(c context.Context, path string, pod *v1.Pod, logRequest *rest.Request) {
	logStream, err := logRequest.Stream(c)
	defer logStream.Close()
	if err != nil {
		panic(err)
	}

	fileName := filepath.Join(path, fmt.Sprintf("pod_%s.txt", pod.Name))
	fmt.Printf("Writing to file %s\n", fileName)

	out, err := os.Create(fileName)
	defer out.Close()
	if err != nil {
		fmt.Printf("Error while creating file %s: %s\n", fileName, err.Error())
		panic(err)
	}

	_, err = io.Copy(out, logStream)
	if err != nil {
		fmt.Printf("Error while writing logs to %s: %s\n", fileName, err.Error())
		_, err := out.WriteString(fmt.Sprintf("kpersist: error occurred while writing this log file: %s", err.Error()))
		if err != nil {
			panic(err)
		}
		err = out.Sync()
		if err != nil {
			panic(err)
		}
		panic(err)
	}

	err = out.Sync()
	if err != nil {
		panic(err)
	}
}
