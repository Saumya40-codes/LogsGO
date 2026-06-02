package store

import (
	"encoding/base64"
	"encoding/json"
	"sort"
	"strings"
)

const customLabelSeparator = byte(0xff)

func emptyLabels() Labels {
	return Labels{
		Services:     make(map[string]int),
		Levels:       make(map[string]int),
		CustomLabels: make(map[string]map[string]int),
	}
}

func normalizeCustomLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		key := strings.TrimSpace(k)
		if key == "" || isReservedLabel(key) {
			continue
		}
		out[key] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func isReservedLabel(label string) bool {
	switch strings.ToLower(strings.TrimSpace(label)) {
	case "service", "level", "message":
		return true
	default:
		return false
	}
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

func labelsFingerprint(labels map[string]string) string {
	labels = normalizeCustomLabels(labels)
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(customLabelSeparator)
		b.WriteString(labels[k])
		b.WriteByte(customLabelSeparator)
	}
	return b.String()
}

func labelsFromFingerprint(fingerprint string) map[string]string {
	if fingerprint == "" {
		return nil
	}
	parts := strings.Split(fingerprint, string(customLabelSeparator))
	if len(parts) < 2 {
		return nil
	}
	labels := make(map[string]string)
	for i := 0; i+1 < len(parts); i += 2 {
		if parts[i] == "" {
			continue
		}
		labels[parts[i]] = parts[i+1]
	}
	return labels
}

func encodeLabels(labels map[string]string) string {
	labels = normalizeCustomLabels(labels)
	if len(labels) == 0 {
		return ""
	}
	data, err := json.Marshal(labels)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeLabels(encoded string) map[string]string {
	if encoded == "" {
		return nil
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil
	}
	var labels map[string]string
	if err := json.Unmarshal(data, &labels); err != nil {
		return nil
	}
	return normalizeCustomLabels(labels)
}

func incMetaLabels(labels *Labels, service, level string, custom map[string]string) {
	if labels.Services == nil {
		labels.Services = make(map[string]int)
	}
	if labels.Levels == nil {
		labels.Levels = make(map[string]int)
	}
	if labels.CustomLabels == nil {
		labels.CustomLabels = make(map[string]map[string]int)
	}

	labels.Services[service]++
	labels.Levels[level]++
	for k, v := range normalizeCustomLabels(custom) {
		if labels.CustomLabels[k] == nil {
			labels.CustomLabels[k] = make(map[string]int)
		}
		labels.CustomLabels[k][v]++
	}
}

func decMetaLabels(labels *Labels, service, level string, custom map[string]string) {
	if labels.Services != nil {
		labels.Services[service]--
		if labels.Services[service] <= 0 {
			delete(labels.Services, service)
		}
	}
	if labels.Levels != nil {
		labels.Levels[level]--
		if labels.Levels[level] <= 0 {
			delete(labels.Levels, level)
		}
	}
	for k, v := range normalizeCustomLabels(custom) {
		if labels.CustomLabels == nil || labels.CustomLabels[k] == nil {
			continue
		}
		labels.CustomLabels[k][v]--
		if labels.CustomLabels[k][v] <= 0 {
			delete(labels.CustomLabels[k], v)
		}
		if len(labels.CustomLabels[k]) == 0 {
			delete(labels.CustomLabels, k)
		}
	}
}

func mergeLabels(dst, src *Labels) {
	if dst.Services == nil {
		dst.Services = make(map[string]int)
	}
	if dst.Levels == nil {
		dst.Levels = make(map[string]int)
	}
	if dst.CustomLabels == nil {
		dst.CustomLabels = make(map[string]map[string]int)
	}
	for service := range src.Services {
		if _, exists := dst.Services[service]; !exists {
			dst.Services[service] = 0
		}
	}
	for level := range src.Levels {
		if _, exists := dst.Levels[level]; !exists {
			dst.Levels[level] = 0
		}
	}
	for label, values := range src.CustomLabels {
		if dst.CustomLabels[label] == nil {
			dst.CustomLabels[label] = make(map[string]int)
		}
		for value := range values {
			if _, exists := dst.CustomLabels[label][value]; !exists {
				dst.CustomLabels[label][value] = 0
			}
		}
	}
}
