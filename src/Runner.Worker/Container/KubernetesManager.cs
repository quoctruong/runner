using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using GitHub.DistributedTask.Pipelines.ContextData;
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using k8s;
using k8s.Models;

namespace GitHub.Runner.Worker.Container
{
    [ServiceLocator(Default = typeof(KubernetesManager))]
    public interface IKubernetesManager : IRunnerService
    {
        Task PrepareJobAsync(IExecutionContext context, List<ContainerInfo> containers);
        Task CleanupJobAsync(IExecutionContext context, List<ContainerInfo> containers);
    }

    public class KubernetesManager : RunnerService, IKubernetesManager
    {
        public async Task PrepareJobAsync(IExecutionContext context, List<ContainerInfo> containers)
        {
            Trace.Entering();
            var jobContainer = containers.Where(c => c.IsJobContainer).SingleOrDefault();
            if (jobContainer == null)
            {
                throw new InvalidOperationException("Job container is required.");
            }

            var podName = $"runner-job-{Guid.NewGuid().ToString().Substring(0, 8)}";
            jobContainer.ContainerId = podName;
            context.JobContext.Container["id"] = new StringContextData(podName);

            var runnerPodName = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_POD_NAME");
            bool isMtlsEnabled = !string.IsNullOrEmpty(runnerPodName) && Directory.Exists("/etc/certs");
            var mtlsSecretName = "certs-" + runnerPodName;
            var templatePath = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_CONTAINER_HOOK_TEMPLATE") ?? "/etc/config/extension.yaml";
            V1Pod templatePod = null;
            if (File.Exists(templatePath))
            {
                try
                {
                    var yamlContent = File.ReadAllText(templatePath);
                    templatePod = k8s.KubernetesYaml.Deserialize<V1Pod>(yamlContent);
                    context.Debug($"Parsed template yaml from {templatePath}. Tolerations count: {templatePod?.Spec?.Tolerations?.Count ?? 0}");
                    if (templatePod?.Spec?.Tolerations != null)
                    {
                        foreach (var t in templatePod.Spec.Tolerations)
                        {
                            context.Debug($"Template toleration: Key={t.Key}, Value={t.Value}, Operator={t.OperatorProperty}, Effect={t.Effect}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    context.Warning($"Warning: Failed to parse pod extension configuration template from {templatePath}: {ex.Message}");
                }
            }

            // Initialize official client with InCluster configuration
            var k8sConfig = KubernetesClientConfiguration.InClusterConfig();
            var client = new Kubernetes(k8sConfig);
            var namespaceVal = k8sConfig.Namespace ?? "default";

            var podVolumes = new List<V1Volume>
            {
                new V1Volume { Name = "work", EmptyDir = new V1EmptyDirVolumeSource() }
            };
            if (isMtlsEnabled)
            {
                podVolumes.Add(new V1Volume
                {
                    Name = "certs-volume",
                    Secret = new V1SecretVolumeSource
                    {
                        SecretName = mtlsSecretName
                    }
                });
            }
            var pod = new V1Pod
            {
                ApiVersion = "v1",
                Kind = "Pod",
                Metadata = new V1ObjectMeta
                {
                    Name = podName,
                    Labels = new Dictionary<string, string> { { "managed-by", "runner" } },
                    Annotations = new Dictionary<string, string>()
                },
                Spec = new V1PodSpec
                {
                    RestartPolicy = "Never",
                    Volumes = podVolumes,
                    Containers = new List<V1Container>()
                }
            };

            // Merge template metadata labels & annotations
            if (templatePod?.Metadata != null)
            {
                if (templatePod.Metadata.Labels != null)
                {
                    foreach (var kvp in templatePod.Metadata.Labels)
                    {
                        pod.Metadata.Labels[kvp.Key] = kvp.Value;
                    }
                }
                if (templatePod.Metadata.Annotations != null)
                {
                    foreach (var kvp in templatePod.Metadata.Annotations)
                    {
                        pod.Metadata.Annotations[kvp.Key] = kvp.Value;
                    }
                }
            }

            // Merge template volumes
            if (templatePod?.Spec?.Volumes != null)
            {
                foreach (var v in templatePod.Spec.Volumes)
                {
                    pod.Spec.Volumes.Add(v);
                }
            }

            // Extract resource limits if defined in template
            V1ResourceRequirements resources = null;
            if (templatePod?.Spec?.Containers != null && templatePod.Spec.Containers.Count > 0)
            {
                resources = templatePod.Spec.Containers[0].Resources;
            }

            // Merge pod-level execution specs
            if (templatePod?.Spec != null)
            {
                pod.Spec.Affinity = templatePod.Spec.Affinity;
                pod.Spec.Tolerations = templatePod.Spec.Tolerations;
                pod.Spec.NodeSelector = templatePod.Spec.NodeSelector;
            }

            var agentImage = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_WORKFLOW_AGENT_IMAGE");
            if (string.IsNullOrEmpty(agentImage))
            {
                agentImage = "us-docker.pkg.dev/ml-oss-artifacts-published/ml-public-container/workflow-agent:latest";
            }

            pod.Spec.InitContainers = new List<V1Container>
            {
                new V1Container
                {
                    Name = "agent-injector",
                    Image = agentImage,
                    Command = new List<string> { "sh", "-c", "cp /bin/workflow-agent /workflow/workflow-agent && cp -r /bin/externals /workflow/externals" },
                    VolumeMounts = new List<V1VolumeMount>
                    {
                        new V1VolumeMount { Name = "work", MountPath = "/workflow" }
                    }
                }
            };

            var agentPort = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_WORKFLOW_AGENT_PORT") ?? "50051";

            var workflowVolumeMounts = new List<V1VolumeMount>
            {
                new V1VolumeMount { Name = "work", MountPath = "/__w" },
                new V1VolumeMount { Name = "work", MountPath = "/__e", SubPath = "externals" },
                new V1VolumeMount { Name = "work", MountPath = "/github/home", SubPath = "_temp/_github_home" },
                new V1VolumeMount { Name = "work", MountPath = "/github/workflow", SubPath = "_temp/_github_workflow" }
            };
            if (isMtlsEnabled)
            {
                workflowVolumeMounts.Add(new V1VolumeMount
                {
                    Name = "certs-volume",
                    MountPath = "/etc/certs",
                    ReadOnlyProperty = true
                });
            }
            // Build job container spec
            var workflowContainer = new V1Container
            {
                Name = "job",
                Image = jobContainer.ContainerImage,
                Command = new List<string> { "/__w/workflow-agent" },
                Args = new List<string> { "--port", agentPort },
                VolumeMounts = workflowVolumeMounts,
                Resources = resources
            };
            pod.Spec.Containers.Add(workflowContainer);

            context.Debug($"Final pod spec tolerations count: {pod.Spec.Tolerations?.Count ?? 0}");
            if (pod.Spec.Tolerations != null)
            {
                foreach (var t in pod.Spec.Tolerations)
                {
                    context.Debug($"Final pod toleration: Key={t.Key}, Value={t.Value}, Operator={t.OperatorProperty}, Effect={t.Effect}");
                }
            }

            context.Debug($"Creating GKE workflow pod {podName} using Kubernetes C# client...");
            context.Output($"Creating workflow pod '{podName}'...");
            await client.CoreV1.CreateNamespacedPodAsync(pod, namespaceVal);

            context.Debug($"Workflow pod {podName} created successfully. Waiting for readiness and Pod IP...");
            context.Output($"Workflow pod '{podName}' created successfully. Waiting for readiness and Pod IP...");

            // Wait for readiness and resolve IP
            string podIP = null;
            string lastPhase = null;
            var timeout = DateTime.UtcNow.AddMinutes(5);
            while (DateTime.UtcNow < timeout)
            {
                await Task.Delay(2000);
                var polledPod = await client.CoreV1.ReadNamespacedPodStatusAsync(podName, namespaceVal);
                var phase = polledPod.Status?.Phase;
                var ip = polledPod.Status?.PodIP;

                context.Debug($"Workflow pod {podName} phase: {phase}, IP: {ip}");
                if (phase != lastPhase)
                {
                    context.Output($"Pod status: {phase}");
                    lastPhase = phase;
                }

                if (string.Equals(phase, "Running", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(ip))
                {
                    podIP = ip;
                    break;
                }
                else if (string.Equals(phase, "Failed", StringComparison.OrdinalIgnoreCase) || 
                         string.Equals(phase, "Unknown", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException($"Workflow pod entered failed phase: {phase}");
                }
            }

            if (string.IsNullOrEmpty(podIP))
            {
                throw new TimeoutException($"Timed out waiting for GKE workflow pod {podName} to come online and resolve IP.");
            }

            jobContainer.ContainerIP = podIP;
            jobContainer.IsAlpine = false;
            context.Debug($"Workflow pod resolved ContainerIP: {podIP}");
        }

        public async Task CleanupJobAsync(IExecutionContext context, List<ContainerInfo> containers)
        {
            Trace.Entering();
            var jobContainer = containers.Where(c => c.IsJobContainer).SingleOrDefault();
            if (jobContainer == null || string.IsNullOrEmpty(jobContainer.ContainerId))
            {
                return;
            }

            var podName = jobContainer.ContainerId;
            context.Debug($"Native GKE pod cleanup triggered. Deleting workflow pod: {podName}");

            try
            {
                var k8sConfig = KubernetesClientConfiguration.InClusterConfig();
                var client = new Kubernetes(k8sConfig);
                var namespaceVal = k8sConfig.Namespace ?? "default";

                await client.CoreV1.DeleteNamespacedPodAsync(podName, namespaceVal);
                context.Debug($"Successfully deleted GKE workflow pod: {podName}");
            }
            catch (Exception ex)
            {
                context.Warning($"Warning: Failed to delete GKE workflow pod {podName}: {ex.Message}");
            }
            await Task.CompletedTask;
        }
    }
}
