package podagent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/doublecloud/transfer/library/go/httputil/headers"
	"github.com/doublecloud/transfer/library/go/yandex/deploy/podagent"
	"github.com/stretchr/testify/require"
)

func TestClient_PodStatus(t *testing.T) {
	type TestCase struct {
		bootstrap   func(t *testing.T) *httptest.Server
		expectedRsp podagent.PodStatusResponse
		willFail    bool
	}

	cases := map[string]TestCase{
		"net_error": {
			bootstrap: func(_ *testing.T) *httptest.Server {
				var srv *httptest.Server
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					srv.CloseClientConnections()
				}))
				return srv
			},
			willFail: true,
		},
		"internal_error": {
			bootstrap: func(_ *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
			},
			willFail: true,
		},
		"not_found": {
			bootstrap: func(_ *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				}))
			},
			willFail: true,
		},
		"success": {
			bootstrap: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "/pod_status", r.URL.Path)

					w.Header().Set(headers.ContentTypeKey, headers.TypeApplicationJSON.String())
					_, _ = w.Write([]byte(`
					{
					  "boxes": [
						{
						  "id": "Box",
						  "revision": 3,
						  "state": 6
						},
						{
						  "id": "tvm_box",
						  "revision": 2,
						  "state": 7
						}
					  ],
					  "workloads": [
						{
						  "id": "Box-Workload",
						  "revision": 3,
						  "state": 4
						},
						{
						  "id": "tvm_workload",
						  "revision": 2,
						  "state": 5
						}
					  ]
					}
					`))
				}))
			},
			expectedRsp: podagent.PodStatusResponse{
				Boxes: []podagent.BoxStatus{
					{
						ID:       "Box",
						Revision: 3,
						State:    6,
					},
					{
						ID:       "tvm_box",
						Revision: 2,
						State:    7,
					},
				},
				Workloads: []podagent.WorkloadStatus{
					{
						ID:       "Box-Workload",
						Revision: 3,
						State:    4,
					},
					{
						ID:       "tvm_workload",
						Revision: 2,
						State:    5,
					},
				},
			},
			willFail: false,
		},
		"success_no_ct": {
			bootstrap: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "/pod_status", r.URL.Path)

					w.Header().Set(headers.ContentTypeKey, "")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`
					{
					  "boxes": [
						{
						  "id": "Box",
						  "revision": 3,
						  "state": 6
						}
					  ],
					  "workloads": [
						{
						  "id": "Box-Workload",
						  "revision": 3,
						  "state": 4
						}
					  ]
					}
					`))
				}))
			},
			expectedRsp: podagent.PodStatusResponse{
				Boxes: []podagent.BoxStatus{
					{
						ID:       "Box",
						Revision: 3,
						State:    6,
					},
				},
				Workloads: []podagent.WorkloadStatus{
					{
						ID:       "Box-Workload",
						Revision: 3,
						State:    4,
					},
				},
			},
			willFail: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := tc.bootstrap(t)
			defer srv.Close()

			client := podagent.NewClient(podagent.WithEndpoint(srv.URL))
			rsp, err := client.PodStatus(context.Background())
			if tc.willFail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedRsp, rsp)
			}
		})
	}
}

func TestClient_PodAttributes(t *testing.T) {
	type TestCase struct {
		bootstrap   func(t *testing.T) *httptest.Server
		expectedRsp podagent.PodAttributesResponse
		checkRsp    func(t *testing.T, rsp podagent.PodAttributesResponse)
		willFail    bool
	}

	cases := map[string]TestCase{
		"net_error": {
			bootstrap: func(_ *testing.T) *httptest.Server {
				var srv *httptest.Server
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					srv.CloseClientConnections()
				}))
				return srv
			},
			willFail: true,
		},
		"internal_error": {
			bootstrap: func(_ *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
			},
			willFail: true,
		},
		"not_found": {
			bootstrap: func(_ *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				}))
			},
			willFail: true,
		},
		"success": {
			bootstrap: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "/pod_attributes", r.URL.Path)

					w.Header().Set(headers.ContentTypeKey, headers.TypeApplicationJSON.String())
					_, _ = w.Write([]byte(`
					{
					  "box_resource_requirements": {
						"Box": {
						  "cpu": {
							"cpu_guarantee_millicores": 0,
							"cpu_limit_millicores": 0
						  },
						  "memory": {
							"memory_guarantee_bytes": 0,
							"memory_limit_bytes": 0
						  }
						},
						"tvm_box": {
						  "cpu": {
							"cpu_guarantee_millicores": 99.0,
							"cpu_limit_millicores": 99.0
						  },
						  "memory": {
							"memory_guarantee_bytes": 100,
							"memory_limit_bytes": 31457280
						  }
						}
					  },
					  "metadata": {
					  },
					  "node_meta": {
						"cluster": "man.yp.yandex.net",
						"dc": "man",
						"fqdn": "man2-9442.search.yandex.net"
					  },
					  "resource_requirements": {
						"cpu": {
						  "cpu_guarantee_millicores": 596.0,
						  "cpu_limit_millicores": 596.0
						},
						"memory": {
						  "memory_guarantee_bytes": 1642070016,
						  "memory_limit_bytes": 1642070016
						}
					  }
					}
					`))
				}))
			},
			expectedRsp: podagent.PodAttributesResponse{
				NodeMeta: podagent.NodeMeta{
					DC:      "man",
					FQDN:    "man2-9442.search.yandex.net",
					Cluster: "man.yp.yandex.net",
				},
				PodMeta: podagent.PodMeta{},
				BoxesRequirements: map[string]podagent.ResourceRequirements{
					"Box": {
						CPU: podagent.CPUResource{
							Guarantee: 0.0,
							Limit:     0.0,
						},
						Memory: podagent.MemoryResource{
							Guarantee: 0,
							Limit:     0,
						},
					},
					"tvm_box": {
						CPU: podagent.CPUResource{
							Guarantee: 99.0,
							Limit:     99.0,
						},
						Memory: podagent.MemoryResource{
							Guarantee: 100,
							Limit:     31457280,
						},
					},
				},
				PodRequirements: podagent.ResourceRequirements{
					CPU: podagent.CPUResource{
						Guarantee: 596.0,
						Limit:     596.0,
					},
					Memory: podagent.MemoryResource{
						Guarantee: 1642070016,
						Limit:     1642070016,
					},
				},
			},
			willFail: false,
		},
		"success_meta": {
			bootstrap: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "/pod_attributes", r.URL.Path)

					w.Header().Set(headers.ContentTypeKey, headers.TypeApplicationJSON.String())
					_, _ = w.Write([]byte(`
					{
					"metadata": {
						"annotations": {
							"lol": "kek"
						},
						"labels": {
						  "deploy_engine": "RSC"
						},
						"pod_id": "my-pod",
						"pod_set_id": "my-podset"
					  },
					  "node_meta": {
						"cluster": "yp-cluster",
						"dc": "dc",
						"fqdn": "my-node.search.yandex.net"
					  }
					}
					`))
				}))
			},
			checkRsp: func(t *testing.T, rsp podagent.PodAttributesResponse) {
				expectedNodeMeta := podagent.NodeMeta{
					Cluster: "yp-cluster",
					DC:      "dc",
					FQDN:    "my-node.search.yandex.net",
				}
				require.Equal(t, expectedNodeMeta, rsp.NodeMeta)
				require.Empty(t, rsp.BoxesRequirements)
				require.Empty(t, rsp.PodRequirements)

				require.Equal(t, "my-pod", rsp.PodMeta.PodID)
				require.Equal(t, "my-podset", rsp.PodMeta.PodSetID)

				type Annotations struct {
					Lol string `json:"lol"`
				}

				var annotations Annotations
				err := json.Unmarshal(rsp.PodMeta.Annotations, &annotations)
				require.NoError(t, err)
				require.Equal(t, Annotations{Lol: "kek"}, annotations)

				type Labels struct {
					DeployEngine string `json:"deploy_engine"`
				}
				var labels Labels
				err = json.Unmarshal(rsp.PodMeta.Labels, &labels)
				require.NoError(t, err)
				require.Equal(t, Labels{DeployEngine: "RSC"}, labels)
			},
			willFail: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := tc.bootstrap(t)
			defer srv.Close()

			client := podagent.NewClient(podagent.WithEndpoint(srv.URL))
			rsp, err := client.PodAttributes(context.Background())
			switch {
			case tc.willFail:
				require.Error(t, err)
			case tc.checkRsp != nil:
				require.NoError(t, err)
				tc.checkRsp(t, rsp)
			default:
				require.NoError(t, err)
				require.Equal(t, tc.expectedRsp, rsp)
			}
		})
	}
}
