package route

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	independentError := errors.New("independent transport failure")
	for _, initialDegraded := range []bool{false, true} {
		for _, routeName := range []string{api.OpenShiftConsoleRouteName, api.OpenShiftConsoleDownloadsRouteName} {
			for _, test := range []struct {
				name            string
				cancelOnDelete  bool
				cancelOnGet     bool
				cancelCause     error
				deadlineExpired bool
				cancelAtEOF     bool
				transportError  error
				deleteStatus    int
				wantError       error
				wantDegraded    bool
			}{
				{name: "canceled delete", cancelOnDelete: true, wantError: context.Canceled},
				{name: "canceled custom route get", cancelOnGet: true, wantError: context.Canceled, wantDegraded: true},
				{name: "wrapped canceled delete", cancelOnDelete: true, transportError: fmt.Errorf("delete: %w", context.Canceled), wantError: context.Canceled},
				{name: "canceled delete with custom cause", cancelOnDelete: true, cancelCause: independentError, wantError: context.Canceled},
				{name: "custom cause is not cancellation", cancelOnDelete: true, cancelCause: independentError, transportError: independentError, wantError: independentError, wantDegraded: true},
				{name: "expired deadline", deadlineExpired: true, wantError: context.DeadlineExceeded},
				{name: "forbidden delete", deleteStatus: http.StatusForbidden, wantDegraded: true},
				{name: "forbidden delete with canceled context", deleteStatus: http.StatusForbidden, cancelAtEOF: true, wantDegraded: true},
				{name: "transport canceled with live context", transportError: context.Canceled, wantError: context.Canceled, wantDegraded: true},
				{name: "independent failure with canceled context", cancelOnDelete: true, transportError: independentError, wantError: independentError, wantDegraded: true},
				{name: "mixed failure with canceled context", cancelOnDelete: true, transportError: errors.Join(independentError, context.Canceled), wantError: independentError, wantDegraded: true},
				{name: "wrapped mixed failure with canceled context", cancelOnDelete: true, transportError: fmt.Errorf("delete: %w", errors.Join(independentError, context.Canceled)), wantError: independentError, wantDegraded: true},
				{name: "lost cancellation wrapping", cancelOnDelete: true, transportError: fmt.Errorf("delete: %v", context.Canceled), wantDegraded: true},
				{name: "mismatched cancellation", cancelOnDelete: true, transportError: context.DeadlineExceeded, wantError: context.DeadlineExceeded, wantDegraded: true},
				{name: "missing custom route", deleteStatus: http.StatusNotFound},
			} {
				t.Run(fmt.Sprintf("%s/%s/initialDegraded=%t", routeName, test.name, initialDegraded), func(t *testing.T) {
					t.Parallel()
					var ctx context.Context
					var cancel context.CancelFunc
					if test.deadlineExpired {
						ctx, cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
					} else if test.cancelCause != nil {
						var cancelWithCause context.CancelCauseFunc
						ctx, cancelWithCause = context.WithCancelCause(context.Background())
						cancel = func() { cancelWithCause(test.cancelCause) }
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
					if test.cancelOnGet {
						ingress.Spec.ComponentRoutes = []configv1.ComponentRouteSpec{{
							Name: routeName, Namespace: api.OpenShiftConsoleNamespace, Hostname: "custom.apps.example.com",
						}}
					}
					defaultRoute := routesub.NewRouteConfig(operatorConfig, ingress, routeName).DefaultRoute(nil, ingress)
					defaultRoute.Status.Ingress = []routev1.RouteIngress{{
						Host: defaultRoute.Spec.Host,
						Conditions: []routev1.RouteIngressCondition{{
							Type: routev1.RouteAdmitted, Status: "True",
						}},
					}}
					customRoutePath := "/apis/route.openshift.io/v1/namespaces/openshift-console/routes/" + routesub.GetCustomRouteName(routeName)
					defaultRequests := 0
					deleteRequests := 0
					customGetRequests := 0
					recovering := false
					httpClient := &http.Client{Transport: routeSyncRoundTripper(func(request *http.Request) (*http.Response, error) {
						var responseObject interface{}
						responseCode := http.StatusOK
						switch {
						case request.Method == http.MethodGet && request.URL.Path == customRoutePath && test.cancelOnGet:
							customGetRequests++
							cancel()
							return nil, ctx.Err()
						case request.Method == http.MethodDelete && request.URL.Path == customRoutePath:
							deleteRequests++
							if test.cancelOnDelete && !recovering {
								cancel()
							}
							if test.transportError != nil && !recovering {
								return nil, test.transportError
							}
							if request.Context().Err() != nil {
								return nil, request.Context().Err()
							}
							responseCode = test.deleteStatus
							if recovering {
								responseCode = http.StatusNotFound
							}
							reason := metav1.StatusReasonForbidden
							if responseCode == http.StatusNotFound {
								reason = metav1.StatusReasonNotFound
							}
							responseObject = &metav1.Status{
								TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
								Status:   metav1.StatusFailure, Reason: reason, Code: int32(responseCode),
								Message: "custom route delete failed",
							}
						case request.Method == http.MethodGet && request.URL.Path == strings.TrimSuffix(customRoutePath, "-custom"):
							defaultRequests++
							responseObject = defaultRoute
						default:
							t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
							return nil, errors.New("unexpected request")
						}
						data, err := json.Marshal(responseObject)
						if err != nil {
							t.Fatal(err)
						}
						body := &routeSyncResponseBody{Reader: strings.NewReader(string(data))}
						if test.cancelAtEOF {
							body.cancel = cancel
						}
						return &http.Response{StatusCode: responseCode, Header: http.Header{"Content-Type": {"application/json"}}, Body: body, Request: request}, nil
					})}
					routeClient, err := routeclientv1.NewForConfigAndClient(&rest.Config{Host: "https://unused.invalid"}, httpClient)
					if err != nil {
						t.Fatal(err)
					}
					recordingClient := &routeSyncRecordingClient{RoutesGetter: routeClient}
					typePrefix := strings.Title(routeName) + "CustomRouteSync"
					degraded, upgradeable := operatorv1.ConditionFalse, operatorv1.ConditionTrue
					if initialDegraded {
						degraded, upgradeable = operatorv1.ConditionTrue, operatorv1.ConditionFalse
					}
					initialStatus := &operatorv1.OperatorStatus{ObservedGeneration: 17, Conditions: []operatorv1.OperatorCondition{
						{Type: typePrefix + "Degraded", Status: degraded, Reason: "Existing", Message: "previous status"},
						{Type: typePrefix + "Upgradeable", Status: upgradeable, Reason: "Existing", Message: "previous status"},
						{Type: typePrefix + "Progressing", Status: operatorv1.ConditionTrue, Reason: "Existing"},
						{Type: "UnrelatedDegraded", Status: operatorv1.ConditionTrue, Reason: "UnrelatedFailure"},
					}}
					statusUpdates := 0
					operatorClient := v1helpers.NewFakeOperatorClient(&operatorConfig.Spec.OperatorSpec, initialStatus.DeepCopy(), func(_ string, _ *operatorv1.OperatorStatus) error {
						statusUpdates++
						return nil
					})
					controller := &RouteSyncController{
						routeName:   routeName,
						routeClient: recordingClient, operatorClient: operatorClient,
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
					originalError := recordingClient.deleteError
					if test.cancelOnGet {
						originalError = recordingClient.getError
						if deleteRequests != 0 || customGetRequests != 1 {
							t.Fatalf("expected custom route GET only: deletes=%d customGets=%d", deleteRequests, customGetRequests)
						}
					}
					if syncErr != nil && syncErr != originalError {
						t.Fatalf("original route error replaced: sync=%v original=%v", syncErr, originalError)
					}
					switch {
					case test.wantError != nil:
						if !errors.Is(syncErr, test.wantError) {
							t.Fatalf("expected %v, got %v", test.wantError, syncErr)
						}
					case test.deleteStatus == http.StatusForbidden:
						if !apierrors.IsForbidden(syncErr) || errors.Is(syncErr, context.Canceled) {
							t.Fatalf("expected forbidden error, got %v", syncErr)
						}
					case test.wantDegraded:
						if !errors.Is(syncErr, test.transportError) {
							t.Fatalf("expected original transport error, got %v", syncErr)
						}
					default:
						if syncErr != nil {
							t.Fatalf("expected successful sync after missing custom route, got %v", syncErr)
						}
					}
					if syncErr != nil && defaultRequests != 0 {
						t.Fatalf("default route accessed after custom route failure: %d requests", defaultRequests)
					}
					if syncErr == nil && defaultRequests == 0 {
						t.Fatal("default route not reconciled after missing custom route")
					}
					contextEnded := test.cancelOnDelete || test.cancelOnGet || test.deadlineExpired || test.cancelAtEOF
					if (ctx.Err() != nil) != contextEnded {
						t.Fatalf("unexpected reconciliation context error: %v", ctx.Err())
					}
					_, actualStatus, _, err := operatorClient.GetOperatorState()
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("context=%v forbidden=%t cancellation=%t statusWrites=%d existingUnchanged=%t", ctx.Err(), apierrors.IsForbidden(syncErr), errors.Is(syncErr, context.Canceled), statusUpdates, reflect.DeepEqual(initialStatus, actualStatus))
					if contextEnded && !test.wantDegraded {
						if statusUpdates != 0 || !reflect.DeepEqual(initialStatus, actualStatus) {
							t.Fatalf("canceled reconciliation published status: updates=%d, conditions=%+v", statusUpdates, actualStatus.Conditions)
						}
						recovering = true
						previousDeletes := deleteRequests
						freshCtx, freshCancel := context.WithCancel(context.Background())
						defer freshCancel()
						if syncErr = controller.Sync(freshCtx, nil); syncErr != nil {
							t.Fatalf("fresh-context reconciliation failed: %v", syncErr)
						}
						if freshCtx.Err() != nil || ctx.Err() == nil || deleteRequests != previousDeletes+1 || defaultRequests != 1 {
							t.Fatalf("incorrect recovery: fresh=%v original=%v deletes=%d defaultGets=%d", freshCtx.Err(), ctx.Err(), deleteRequests, defaultRequests)
						}
						_, actualStatus, _, err = operatorClient.GetOperatorState()
						if err != nil {
							t.Fatal(err)
						}
						t.Logf("fresh-context recovery: statusWrites=%d defaultGets=%d", statusUpdates, defaultRequests)
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
						wantReason := "FailedDeleteCustomRoutes"
						if test.cancelOnGet {
							wantReason = "FailedCustomRouteApply"
						}
						if condition.Reason != wantReason || condition.Message != syncErr.Error() {
							t.Fatalf("expected detailed route failure, got %+v", condition)
						}
					} else if condition.Reason != "" || condition.Message != "" {
						t.Fatalf("successful reconciliation retained failure details: %+v", condition)
					}
					if condition.Status != wantStatus {
						t.Fatalf("expected Degraded=%s, got %+v", wantStatus, condition)
					}
					upgradeableCondition := v1helpers.FindOperatorCondition(actualStatus.Conditions, typePrefix+"Upgradeable")
					wantUpgradeable := operatorv1.ConditionTrue
					if test.wantDegraded {
						wantUpgradeable = operatorv1.ConditionFalse
					}
					if upgradeableCondition == nil || upgradeableCondition.Status != wantUpgradeable || upgradeableCondition.Reason != condition.Reason || upgradeableCondition.Message != condition.Message {
						t.Fatalf("incorrect Upgradeable condition: %+v", upgradeableCondition)
					}
					progressing := v1helpers.FindOperatorCondition(actualStatus.Conditions, typePrefix+"Progressing")
					if progressing == nil || progressing.Status != operatorv1.ConditionFalse {
						t.Fatalf("incorrect Progressing condition: %+v", progressing)
					}
					if actualStatus.ObservedGeneration != initialStatus.ObservedGeneration || !reflect.DeepEqual(v1helpers.FindOperatorCondition(actualStatus.Conditions, "UnrelatedDegraded"), v1helpers.FindOperatorCondition(initialStatus.Conditions, "UnrelatedDegraded")) {
						t.Fatal("unrelated status changed")
					}
				})
			}
		}
	}
}

type routeSyncResponseBody struct {
	io.Reader
	cancel context.CancelFunc
}

func (body *routeSyncResponseBody) Read(buffer []byte) (int, error) {
	count, err := body.Reader.Read(buffer)
	if err == io.EOF && body.cancel != nil {
		body.cancel()
	}
	return count, err
}

func (body *routeSyncResponseBody) Close() error { return nil }

type routeSyncRecordingClient struct {
	routeclientv1.RoutesGetter
	deleteError error
	getError    error
}

func (client *routeSyncRecordingClient) Routes(namespace string) routeclientv1.RouteInterface {
	return &routeSyncRecordingRoutes{RouteInterface: client.RoutesGetter.Routes(namespace), deleteError: &client.deleteError, getError: &client.getError}
}

type routeSyncRecordingRoutes struct {
	routeclientv1.RouteInterface
	deleteError *error
	getError    *error
}

func (routes *routeSyncRecordingRoutes) Get(ctx context.Context, name string, options metav1.GetOptions) (*routev1.Route, error) {
	route, err := routes.RouteInterface.Get(ctx, name, options)
	*routes.getError = err
	return route, err
}

func (routes *routeSyncRecordingRoutes) Delete(ctx context.Context, name string, options metav1.DeleteOptions) error {
	err := routes.RouteInterface.Delete(ctx, name, options)
	*routes.deleteError = err
	return err
}

func TestIsCancellationOnly(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		err        error
		contextErr error
		want       bool
	}{
		{name: "nil error", contextErr: context.Canceled},
		{name: "live context", err: context.Canceled},
		{name: "canceled", err: context.Canceled, contextErr: context.Canceled, want: true},
		{name: "wrapped deadline", err: fmt.Errorf("delete: %w", context.DeadlineExceeded), contextErr: context.DeadlineExceeded, want: true},
		{name: "different cancellation", err: context.Canceled, contextErr: context.DeadlineExceeded},
		{name: "lost wrapping", err: fmt.Errorf("delete: %v", context.Canceled), contextErr: context.Canceled},
		{name: "mixed join", err: errors.Join(errors.New("failure"), context.Canceled), contextErr: context.Canceled},
		{name: "wrapped mixed join", err: fmt.Errorf("delete: %w", errors.Join(context.Canceled, errors.New("failure"))), contextErr: context.Canceled},
		{name: "conservatively retain aggregates", err: errors.Join(context.Canceled), contextErr: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := isCancellationOnly(test.err, test.contextErr); got != test.want {
				t.Fatalf("isCancellationOnly(%v, %v) = %t, want %t", test.err, test.contextErr, got, test.want)
			}
		})
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
