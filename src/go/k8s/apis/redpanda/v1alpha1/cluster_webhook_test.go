// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1_test

import (
	"testing"

	cmmeta "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/pointer"
)

func TestValidateUpdate(t *testing.T) {
	var replicas1 int32 = 1
	var replicas2 int32 = 2

	redpandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "",
		},
		Spec: v1alpha1.ClusterSpec{
			Replicas: pointer.Int32Ptr(replicas2),
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI: v1alpha1.SocketAddress{Port: 123},
			},
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
			ExternalConnectivity: v1alpha1.ExternalConnectivityConfig{
				Enabled:   false,
				Subdomain: "",
			},
		},
	}

	updatedCluster := redpandaCluster.DeepCopy()
	updatedCluster.Spec.Replicas = &replicas1
	updatedCluster.Spec.Configuration = v1alpha1.RedpandaConfig{
		KafkaAPI: v1alpha1.SocketAddress{Port: 1234},
		TLS: v1alpha1.TLSConfig{
			KafkaAPI: v1alpha1.KafkaAPITLS{
				RequireClientAuth: true,
				IssuerRef: &cmmeta.ObjectReference{
					Name: "test",
				},
				NodeSecretRef: &corev1.ObjectReference{
					Name:      "name",
					Namespace: "default",
				},
				Enabled: false,
			},
		},
	}

	err := updatedCluster.ValidateUpdate(redpandaCluster)
	if err == nil {
		t.Fatalf("expecting validation error but got none")
	}

	// verify the error causes contain all expected fields
	statusError := err.(*apierrors.StatusError)
	expectedFields := []string{
		field.NewPath("spec").Child("replicas").String(),
		field.NewPath("spec").Child("resources").Child("limits").Child("memory").String(),
		field.NewPath("spec").Child("configuration").Child("tls").Child("requireclientauth").String(),
		field.NewPath("spec").Child("configuration").Child("tls").Child("nodeSecretRef").String(),
	}
	for _, ef := range expectedFields {
		found := false
		for _, c := range statusError.Status().Details.Causes {
			if ef == c.Field {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expecting failure on field %s but have %v", ef, statusError.Status().Details.Causes)
		}
	}
}

// nolint:funlen // consider splitting tests
func TestValidateUpdate_NoError(t *testing.T) {
	var replicas2 int32 = 2

	redpandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "",
		},
		Spec: v1alpha1.ClusterSpec{
			Replicas: pointer.Int32Ptr(replicas2),
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI:  v1alpha1.SocketAddress{Port: 123},
				AdminAPI:  v1alpha1.SocketAddress{Port: 125},
				RPCServer: v1alpha1.SocketAddress{Port: 126},
			},
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
			ExternalConnectivity: v1alpha1.ExternalConnectivityConfig{
				Enabled:   false,
				Subdomain: "",
			},
		},
	}

	t.Run("same object updated", func(t *testing.T) {
		err := redpandaCluster.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})

	t.Run("scale up", func(t *testing.T) {
		var scaleUp int32 = *redpandaCluster.Spec.Replicas + 1
		updatedScaleUp := redpandaCluster.DeepCopy()
		updatedScaleUp.Spec.Replicas = &scaleUp
		err := updatedScaleUp.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})

	t.Run("change image and tag", func(t *testing.T) {
		updatedImage := redpandaCluster.DeepCopy()
		updatedImage.Spec.Image = "differentimage"
		updatedImage.Spec.Version = "111"
		err := updatedImage.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})

	t.Run("collision in the port", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.Configuration.KafkaAPI.Port = 200
		updatePort.Spec.Configuration.AdminAPI.Port = 200
		updatePort.Spec.Configuration.RPCServer.Port = 200

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.ExternalConnectivity.Enabled = true
		updatePort.Spec.Configuration.KafkaAPI.Port = 200
		updatePort.Spec.Configuration.AdminAPI.Port = 201
		updatePort.Spec.Configuration.RPCServer.Port = 300

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.ExternalConnectivity.Enabled = true
		updatePort.Spec.Configuration.KafkaAPI.Port = 200
		updatePort.Spec.Configuration.AdminAPI.Port = 300
		updatePort.Spec.Configuration.RPCServer.Port = 201

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.ExternalConnectivity.Enabled = true
		updatePort.Spec.Configuration.KafkaAPI.Port = 200
		updatePort.Spec.Configuration.AdminAPI.Port = 300
		updatePort.Spec.Configuration.RPCServer.Port = 301

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.ExternalConnectivity.Enabled = true
		updatePort.Spec.Configuration.KafkaAPI.Port = 201
		updatePort.Spec.Configuration.AdminAPI.Port = 200
		updatePort.Spec.Configuration.RPCServer.Port = 300

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("requireclientauth true and tls enabled", func(t *testing.T) {
		tls := redpandaCluster.DeepCopy()
		tls.Spec.Configuration.TLS.KafkaAPI.RequireClientAuth = true
		tls.Spec.Configuration.TLS.KafkaAPI.Enabled = true

		err := tls.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})
}

// nolint:funlen // consider splitting tests
func TestCreation(t *testing.T) {
	redpandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "",
		},
		Spec: v1alpha1.ClusterSpec{
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI:  v1alpha1.SocketAddress{Port: 123},
				AdminAPI:  v1alpha1.SocketAddress{Port: 125},
				RPCServer: v1alpha1.SocketAddress{Port: 126},
			},
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("2G"),
				},
			},
			ExternalConnectivity: v1alpha1.ExternalConnectivityConfig{
				Enabled:   false,
				Subdomain: "",
			},
		},
	}

	t.Run("no collision in the port", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.Configuration.KafkaAPI.Port = 200

		err := newPort.ValidateCreate()
		assert.NoError(t, err)
	})

	t.Run("collision in the port", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.Configuration.KafkaAPI.Port = 200
		newPort.Spec.Configuration.AdminAPI.Port = 200
		newPort.Spec.Configuration.RPCServer.Port = 200

		err := newPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.ExternalConnectivity.Enabled = true
		newPort.Spec.Configuration.KafkaAPI.Port = 200
		newPort.Spec.Configuration.AdminAPI.Port = 201
		newPort.Spec.Configuration.RPCServer.Port = 300

		err := newPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.ExternalConnectivity.Enabled = true
		newPort.Spec.Configuration.KafkaAPI.Port = 200
		newPort.Spec.Configuration.AdminAPI.Port = 300
		newPort.Spec.Configuration.RPCServer.Port = 201

		err := newPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.ExternalConnectivity.Enabled = true
		newPort.Spec.Configuration.KafkaAPI.Port = 200
		newPort.Spec.Configuration.AdminAPI.Port = 300
		newPort.Spec.Configuration.RPCServer.Port = 301

		err := newPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.ExternalConnectivity.Enabled = true
		newPort.Spec.Configuration.KafkaAPI.Port = 201
		newPort.Spec.Configuration.AdminAPI.Port = 200
		newPort.Spec.Configuration.RPCServer.Port = 300

		err := newPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("incorrect memory", func(t *testing.T) {
		memory := redpandaCluster.DeepCopy()
		memory.Spec.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		}

		err := memory.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("tls properly configured", func(t *testing.T) {
		tls := redpandaCluster.DeepCopy()
		tls.Spec.Configuration.TLS.KafkaAPI.Enabled = true
		tls.Spec.Configuration.TLS.KafkaAPI.RequireClientAuth = true

		err := tls.ValidateCreate()
		assert.NoError(t, err)
	})

	t.Run("require client auth without tls enabled", func(t *testing.T) {
		tls := redpandaCluster.DeepCopy()
		tls.Spec.Configuration.TLS.KafkaAPI.Enabled = false
		tls.Spec.Configuration.TLS.KafkaAPI.RequireClientAuth = true

		err := tls.ValidateCreate()
		assert.Error(t, err)
	})
}