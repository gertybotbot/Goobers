package operator

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/goobers/goobers/api/v1alpha1"
)

// NewScheme builds a runtime scheme with the core/apps types plus the Goobers
// CRD types the operator reconciles. The v1alpha1 package intentionally ships
// types only (no SchemeBuilder), so registration is done here.
//
// GooberRun and GooberRunAction are the RUNTIME kinds of the Kubernetes-native
// runner (internal/kuberunner). Registering them here does not activate the
// quarantined Temporal engine in internal/engine: the two runners share the
// scheme and nothing else.
func NewScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, err
	}
	if err := batchv1.AddToScheme(s); err != nil {
		return nil, err
	}
	s.AddKnownTypes(v1alpha1.GroupVersion,
		&v1alpha1.Gaggle{}, &v1alpha1.GaggleList{},
		&v1alpha1.Goober{}, &v1alpha1.GooberList{},
		&v1alpha1.GooberRun{}, &v1alpha1.GooberRunList{},
		&v1alpha1.GooberRunAction{}, &v1alpha1.GooberRunActionList{},
	)
	metav1.AddToGroupVersion(s, v1alpha1.GroupVersion)
	return s, nil
}
