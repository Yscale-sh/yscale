package kubectl

import "time"

type ObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	CreationTimestamp time.Time         `json:"creationTimestamp"`
	Labels            map[string]string `json:"labels,omitempty"`
}

type NodeList struct {
	Items []Node `json:"items"`
}

type Node struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		Unschedulable bool   `json:"unschedulable"`
		ProviderID    string `json:"providerID"`
	} `json:"spec"`
	Status struct {
		Capacity    map[string]string `json:"capacity"`
		Allocatable map[string]string `json:"allocatable"`
		Conditions  []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		NodeInfo struct {
			MachineID               string `json:"machineID"`
			SystemUUID              string `json:"systemUUID"`
			BootID                  string `json:"bootID"`
			KernelVersion           string `json:"kernelVersion"`
			OSImage                 string `json:"osImage"`
			ContainerRuntimeVersion string `json:"containerRuntimeVersion"`
			KubeletVersion          string `json:"kubeletVersion"`
			OperatingSystem         string `json:"operatingSystem"`
			Architecture            string `json:"architecture"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

func (n Node) Ready() bool {
	for _, condition := range n.Status.Conditions {
		if condition.Type == "Ready" {
			return condition.Status == "True"
		}
	}
	return false
}

type PodList struct {
	Items []Pod `json:"items"`
}

type Endpoints struct {
	Metadata ObjectMeta `json:"metadata"`
	Subsets  []struct {
		Addresses []struct {
			IP string `json:"ip"`
		} `json:"addresses"`
	} `json:"subsets"`
}

func (e Endpoints) ReadyAddressCount() int {
	count := 0
	for _, subset := range e.Subsets {
		count += len(subset.Addresses)
	}
	return count
}

type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		Phase      string `json:"phase"`
		PodIP      string `json:"podIP"`
		HostIP     string `json:"hostIP"`
		Conditions []struct {
			Type               string    `json:"type"`
			Status             string    `json:"status"`
			Reason             string    `json:"reason,omitempty"`
			Message            string    `json:"message,omitempty"`
			LastTransitionTime time.Time `json:"lastTransitionTime"`
		} `json:"conditions"`
		ContainerStatuses []struct {
			Name    string `json:"name"`
			Ready   bool   `json:"ready"`
			ImageID string `json:"imageID"`
			State   struct {
				Running *struct {
					StartedAt time.Time `json:"startedAt"`
				} `json:"running,omitempty"`
				Terminated *struct {
					ExitCode   int       `json:"exitCode"`
					Reason     string    `json:"reason"`
					Message    string    `json:"message,omitempty"`
					StartedAt  time.Time `json:"startedAt"`
					FinishedAt time.Time `json:"finishedAt"`
				} `json:"terminated,omitempty"`
				Waiting *struct {
					Reason  string `json:"reason"`
					Message string `json:"message,omitempty"`
				} `json:"waiting,omitempty"`
			} `json:"state"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (p Pod) Condition(name string) (time.Time, bool) {
	for _, condition := range p.Status.Conditions {
		if condition.Type == name && condition.Status == "True" {
			return condition.LastTransitionTime, true
		}
	}
	return time.Time{}, false
}

func (p Pod) RunningStartedAt() (time.Time, bool) {
	for _, status := range p.Status.ContainerStatuses {
		if status.State.Running != nil {
			return status.State.Running.StartedAt, true
		}
	}
	return time.Time{}, false
}

type EventList struct {
	Items []Event `json:"items"`
}

type Event struct {
	Metadata       ObjectMeta `json:"metadata"`
	Reason         string     `json:"reason"`
	Message        string     `json:"message"`
	Type           string     `json:"type"`
	EventTime      time.Time  `json:"eventTime"`
	FirstTimestamp time.Time  `json:"firstTimestamp"`
	LastTimestamp  time.Time  `json:"lastTimestamp"`
	Series         *struct {
		LastObservedTime time.Time `json:"lastObservedTime"`
		Count            int       `json:"count"`
	} `json:"series,omitempty"`
}

func (e Event) Timestamp() time.Time {
	if !e.EventTime.IsZero() {
		return e.EventTime
	}
	if !e.LastTimestamp.IsZero() {
		return e.LastTimestamp
	}
	if !e.FirstTimestamp.IsZero() {
		return e.FirstTimestamp
	}
	return e.Metadata.CreationTimestamp
}

type Version struct {
	ClientVersion struct {
		GitVersion string `json:"gitVersion"`
	} `json:"clientVersion"`
	ServerVersion struct {
		GitVersion string `json:"gitVersion"`
	} `json:"serverVersion"`
}
