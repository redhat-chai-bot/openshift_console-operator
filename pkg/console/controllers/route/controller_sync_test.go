package route

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	configv1 "github.com/openshift/api/config/v1"
	operatorv1 "github.com/openshift/api/operator/v1"
	routev1 "github.com/openshift/api/route/v1"
	configlistersv1 "github.com/openshift/client-go/config/listers/config/v1"
	operatorlistersv1 "github.com/openshift/client-go/operator/listers/operator/v1"
	routeclientv1 "github.com/openshift/client-go/route/clientset/versioned/typed/route/v1"
	routelistersv1 "github.com/openshift/client-go/route/listers/route/v1"
	"github.com/openshift/library-go/pkg/operator/v1helpers"

	"github.com/openshift/console-operator/pkg/api"
	routesub "github.com/openshift/console-operator/pkg/console/subresource/route"
)

func TestSyncCustomRouteStatus(t *testing.T) {
	t.Parallel()
	for _, routeName := range []string{api.OpenShiftConsoleRouteName, api.OpenShiftConsoleDownloadsRouteName} {
		for _, test := range []struct {
			name              string
			cancelOnDelete    bool
			deadlineExpired   bool
			initialDegraded   bool
			transportCanceled bool
			deleteStatus      int
			wantError         error
			wantDegraded      bool
		}{
			{name: "canceled delete", cancelOnDelete: true, wantError: context.Canceled},
			{name: "canceled delete preserves failure", cancelOnDelete: true, initialDegraded: true, wantError: context.Canceled},
			{name: "expired deadline", deadlineExpired: true, wantError: context.DeadlineExceeded},
			{name: "forbidden delete", deleteStatus: http.StatusForbidden, wantDegraded: true},
			{name: "transport canceled with live context", transportCanceled: true, wantError: context.Canceled, wantDegraded: true},
			{name: "missing custom route", deleteStatus: http.StatusNotFound},
		} {
			t.Run(routeName+"/"+test.name, func(t *testing.T) {
				t.Parallel()
				var ctx context.Context
				var cancel context.CancelFunc
				if test.deadlineExpired {
					ctx, cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
				} else {
					ctx, cancel = context.WithCancel(context.Background())
				}
				defer cancel()
				operatorConfig := &operatorv1.Console{
					ObjectMeta: metav1.ObjectMeta{Name: api.ConfigResourceName},
					Spec: operatorv1.ConsoleSpec{OperatorSpec: operatorv1.OperatorSpec{
						ManagementState: operatorv1.Managed,
					}},
				}
				ingress := &configv1.Ingress{
					ObjectMeta: metav1.ObjectMeta{Name: api.ConfigResourceName},
					Spec:       configv1.IngressSpec{Domain: "apps.example.com"},
				}
				defaultRoute := routesub.NewRouteConfig(operatorConfig, ingress, routeName).DefaultRoute(nil, ingress)
				defaultRoute.Status.Ingress = []routev1.RouteIngress{{
					Host: defaultRoute.Spec.Host,
					Conditions: []routev1.RouteIngressCondition{{
						Type: routev1.RouteAdmitted, Status: "True",
					}},
				}}
				customRoutePath := "/apis/route.openshift.io/v1/namespaces/openshift-console/routes/" + routesub.GetCustomRouteName(routeName)
				releaseDelete := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Header().Set("Content-Type", "application/json")
					switch {
					case request.Method == http.MethodDelete && request.URL.Path == customRoutePath:
						if test.cancelOnDelete {
							cancel()
							<-releaseDelete
							return
						}
						reason := metav1.StatusReasonForbidden
						if test.deleteStatus == http.StatusNotFound {
							reason = metav1.StatusReasonNotFound
						}
						writer.WriteHeader(test.deleteStatus)
						if err := json.NewEncoder(writer).Encode(&metav1.Status{
							TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
							Status:   metav1.StatusFailure, Reason: reason, Code: int32(test.deleteStatus),
							Message: "custom route delete failed",
						}); err != nil {
							t.Error(err)
						}
					case request.Method == http.MethodGet && request.URL.Path == strings.TrimSuffix(customRoutePath, "-custom"):
						if err := json.NewEncoder(writer).Encode(defaultRoute); err != nil {
							t.Error(err)
						}
					default:
						t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
						http.Error(writer, "unexpected request", http.StatusInternalServerError)
					}
				}))
				defer server.Close()
				defer close(releaseDelete)
				httpClient := server.Client()
				httpClient.Timeout = 10 * time.Second
				if test.transportCanceled {
					httpClient.Transport = routeSyncRoundTripper(func(request *http.Request) (*http.Response, error) {
						if request.Method != http.MethodDelete || request.URL.Path != customRoutePath {
							t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
						}
						return nil, context.Canceled
					})
				}
				routeClient, err := routeclientv1.NewForConfigAndClient(&rest.Config{Host: server.URL}, httpClient)
				if err != nil {
					t.Fatal(err)
				}
				typePrefix := strings.Title(routeName) + "CustomRouteSync"
				initialStatus := &operatorv1.OperatorStatus{Conditions: []operatorv1.OperatorCondition{{
					Type: typePrefix + "Degraded", Status: operatorv1.ConditionFalse,
				}}}
				if test.initialDegraded {
					initialStatus.Conditions = []operatorv1.OperatorCondition{
						{Type: typePrefix + "Degraded", Status: operatorv1.ConditionTrue, Reason: "ExistingFailure", Message: "previous route sync failed"},
						{Type: typePrefix + "Upgradeable", Status: operatorv1.ConditionFalse, Reason: "ExistingFailure", Message: "previous route sync failed"},
					}
				}
				statusUpdates := 0
				operatorClient := v1helpers.NewFakeOperatorClient(&operatorConfig.Spec.OperatorSpec, initialStatus.DeepCopy(), func(_ string, _ *operatorv1.OperatorStatus) error {
					statusUpdates++
					return nil
				})
				controller := &RouteSyncController{
					routeName:   routeName,
					routeClient: routeClient, operatorClient: operatorClient,
					operatorConfigLister: operatorlistersv1.NewConsoleLister(routeSyncTestIndexer(t, operatorConfig)),
					ingressConfigLister:  configlistersv1.NewIngressLister(routeSyncTestIndexer(t, ingress)),
					infrastructureConfigLister: configlistersv1.NewInfrastructureLister(routeSyncTestIndexer(t, &configv1.Infrastructure{
						ObjectMeta: metav1.ObjectMeta{Name: api.ConfigResourceName},
					})),
					clusterVersionLister: configlistersv1.NewClusterVersionLister(routeSyncTestIndexer(t, &configv1.ClusterVersion{
						ObjectMeta: metav1.ObjectMeta{Name: "version"},
					})),
					ingressControllerLister: operatorlistersv1.NewIngressControllerLister(routeSyncTestIndexer(t, &operatorv1.IngressController{
						ObjectMeta: metav1.ObjectMeta{Name: api.DefaultIngressController, Namespace: api.IngressControllerNamespace},
					})),
					routeLister: routelistersv1.NewRouteLister(routeSyncTestIndexer(t)),
				}
				syncErr := controller.Sync(ctx, nil)
				switch {
				case test.wantError != nil:
					if !errors.Is(syncErr, test.wantError) {
						t.Fatalf("expected %v, got %v", test.wantError, syncErr)
					}
				case test.wantDegraded:
					if !apierrors.IsForbidden(syncErr) {
						t.Fatalf("expected forbidden error, got %v", syncErr)
					}
				default:
					if syncErr != nil {
						t.Fatalf("expected successful sync after missing custom route, got %v", syncErr)
					}
				}
				contextEnded := test.cancelOnDelete || test.deadlineExpired
				if (ctx.Err() != nil) != contextEnded {
					t.Fatalf("unexpected reconciliation context error: %v", ctx.Err())
				}
				_, actualStatus, _, err := operatorClient.GetOperatorState()
				if err != nil {
					t.Fatal(err)
				}
				if contextEnded {
					if statusUpdates != 0 || !reflect.DeepEqual(initialStatus, actualStatus) {
						t.Fatalf("canceled reconciliation published status: updates=%d, conditions=%+v", statusUpdates, actualStatus.Conditions)
					}
					return
				}
				if statusUpdates != 1 {
					t.Fatalf("expected one status update, got %d", statusUpdates)
				}
				condition := v1helpers.FindOperatorCondition(actualStatus.Conditions, typePrefix+"Degraded")
				if condition == nil {
					t.Fatal("missing custom route Degraded condition")
				}
				wantStatus := operatorv1.ConditionFalse
				if test.wantDegraded {
					wantStatus = operatorv1.ConditionTrue
					if condition.Reason != "FailedDeleteCustomRoutes" || condition.Message != syncErr.Error() {
						t.Fatalf("expected detailed delete failure, got %+v", condition)
					}
				}
				if condition.Status != wantStatus {
					t.Fatalf("expected Degraded=%s, got %+v", wantStatus, condition)
				}
			})
		}
	}
}

func routeSyncTestIndexer(t *testing.T, objects ...runtime.Object) cache.Indexer {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, object := range objects {
		if err := indexer.Add(object); err != nil {
			t.Fatal(err)
		}
	}
	return indexer
}

type routeSyncRoundTripper func(*http.Request) (*http.Response, error)

func (transport routeSyncRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}
