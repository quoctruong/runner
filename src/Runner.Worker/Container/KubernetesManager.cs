using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using GitHub.DistributedTask.Pipelines.ContextData;
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using Newtonsoft.Json.Linq;

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

            var templatePath = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_CONTAINER_HOOK_TEMPLATE") ?? "/etc/config/extension.yaml";
            if (!File.Exists(templatePath))
            {
                throw new FileNotFoundException($"Kubernetes pod extension config template not found at: {templatePath}");
            }

            var yamlContent = File.ReadAllText(templatePath);
            var deserializer = new YamlDotNet.Serialization.DeserializerBuilder().Build();
            var extensionDoc = deserializer.Deserialize<Dictionary<object, object>>(yamlContent);

            var spec = extensionDoc.ContainsKey("spec") ? extensionDoc["spec"] as Dictionary<object, object> : null;
            var metadata = extensionDoc.ContainsKey("metadata") ? extensionDoc["metadata"] as Dictionary<object, object> : null;

            var namespaceVal = File.Exists("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                ? File.ReadAllText("/var/run/secrets/kubernetes.io/serviceaccount/namespace").Trim()
                : "default";

            var volumes = new List<object>
            {
                new { name = "work", emptyDir = new { } }
            };

            if (spec != null && spec.ContainsKey("volumes"))
            {
                var templateVolumes = spec["volumes"] as List<object>;
                if (templateVolumes != null)
                {
                    foreach (var v in templateVolumes)
                    {
                        volumes.Add(v);
                    }
                }
            }

            var labels = new Dictionary<string, string>
            {
                { "managed-by", "runner" }
            };
            if (metadata != null && metadata.ContainsKey("labels"))
            {
                var templateLabels = metadata["labels"] as Dictionary<object, object>;
                if (templateLabels != null)
                {
                    foreach (var kvp in templateLabels)
                    {
                        labels[kvp.Key.ToString()] = kvp.Value?.ToString();
                    }
                }
            }

            var annotations = new Dictionary<string, string>();
            if (metadata != null && metadata.ContainsKey("annotations"))
            {
                var templateAnnotations = metadata["annotations"] as Dictionary<object, object>;
                if (templateAnnotations != null)
                {
                    foreach (var kvp in templateAnnotations)
                    {
                        annotations[kvp.Key.ToString()] = kvp.Value?.ToString();
                    }
                }
            }

            object jobContainerResources = null;
            if (spec != null && spec.ContainsKey("containers"))
            {
                var templateContainers = spec["containers"] as List<object>;
                if (templateContainers != null && templateContainers.Count > 0)
                {
                    var firstContainer = templateContainers[0] as Dictionary<object, object>;
                    if (firstContainer != null && firstContainer.ContainsKey("resources"))
                    {
                        jobContainerResources = firstContainer["resources"];
                    }
                }
            }

            var workflowContainers = new List<object>
            {
                new
                {
                    name = "job",
                    image = jobContainer.ContainerImage,
                    command = new[] { "/__w/workflow-agent" },
                    args = new[] { "--port", "50051" },
                    volumeMounts = new[]
                    {
                        new { name = "work", mountPath = "/__w", subPath = (string)null },
                        new { name = "work", mountPath = "/__e", subPath = "externals" },
                        new { name = "work", mountPath = "/github/home", subPath = "_temp/_github_home" },
                        new { name = "work", mountPath = "/github/workflow", subPath = "_temp/_github_workflow" }
                    },
                    resources = jobContainerResources
                }
            };

            var podSpecDict = new Dictionary<string, object>
            {
                { "restartPolicy", "Never" },
                { "containers", workflowContainers },
                { "volumes", volumes }
            };

            if (spec != null)
            {
                if (spec.ContainsKey("initContainers"))
                {
                    podSpecDict["initContainers"] = spec["initContainers"];
                }
                if (spec.ContainsKey("affinity"))
                {
                    podSpecDict["affinity"] = spec["affinity"];
                }
                if (spec.ContainsKey("tolerations"))
                {
                    podSpecDict["tolerations"] = spec["tolerations"];
                }
                if (spec.ContainsKey("nodeSelector"))
                {
                    podSpecDict["nodeSelector"] = spec["nodeSelector"];
                }
            }

            var podManifest = new
            {
                apiVersion = "v1",
                kind = "Pod",
                metadata = new
                {
                    name = podName,
                    labels = labels,
                    annotations = annotations
                },
                spec = podSpecDict
            };

            var podJson = Newtonsoft.Json.JsonConvert.SerializeObject(podManifest);
            var token = File.Exists("/var/run/secrets/kubernetes.io/serviceaccount/token")
                ? File.ReadAllText("/var/run/secrets/kubernetes.io/serviceaccount/token").Trim()
                : string.Empty;

            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
            };

            using (var httpClient = new HttpClient(handler))
            {
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                var content = new StringContent(podJson, System.Text.Encoding.UTF8, "application/json");
                var response = await httpClient.PostAsync($"https://kubernetes.default.svc/api/v1/namespaces/{namespaceVal}/pods", content);
                if (!response.IsSuccessStatusCode)
                {
                    var errorMsg = await response.Content.ReadAsStringAsync();
                    throw new InvalidOperationException($"Failed to create GKE workflow pod via native REST API. Status: {response.StatusCode}, Error: {errorMsg}");
                }
            }

            context.Debug($"Workflow pod {podName} created successfully. Waiting for readiness and Pod IP...");

            string podIP = null;
            var timeout = DateTime.UtcNow.AddMinutes(5);
            while (DateTime.UtcNow < timeout)
            {
                await Task.Delay(2000);

                using (var httpClient = new HttpClient(handler))
                {
                    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                    var response = await httpClient.GetAsync($"https://kubernetes.default.svc/api/v1/namespaces/{namespaceVal}/pods/{podName}");
                    if (!response.IsSuccessStatusCode)
                    {
                        context.Debug($"Warning: Failed to poll GKE workflow pod status: {response.StatusCode}");
                        continue;
                    }

                    var jsonStr = await response.Content.ReadAsStringAsync();
                    var podStatus = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonStr);
                    if (podStatus != null && podStatus.ContainsKey("status"))
                    {
                        var statusObj = podStatus["status"] as JObject;
                        if (statusObj != null)
                        {
                            var phase = statusObj["phase"]?.ToString();
                            var ip = statusObj["podIP"]?.ToString();

                            context.Debug($"Workflow pod {podName} phase: {phase}, IP: {ip}");

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
                    }
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

            var token = File.Exists("/var/run/secrets/kubernetes.io/serviceaccount/token")
                ? File.ReadAllText("/var/run/secrets/kubernetes.io/serviceaccount/token").Trim()
                : string.Empty;

            var namespaceVal = File.Exists("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                ? File.ReadAllText("/var/run/secrets/kubernetes.io/serviceaccount/namespace").Trim()
                : "default";

            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
            };

            try
            {
                using (var httpClient = new HttpClient(handler))
                {
                    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                    var response = await httpClient.DeleteAsync($"https://kubernetes.default.svc/api/v1/namespaces/{namespaceVal}/pods/{podName}");
                    if (response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        context.Debug($"Successfully deleted GKE workflow pod: {podName}");
                    }
                    else
                    {
                        var errorMsg = await response.Content.ReadAsStringAsync();
                        context.Warning($"Failed to delete GKE workflow pod {podName}. Status: {response.StatusCode}, Error: {errorMsg}");
                    }
                }
            }
            catch (Exception ex)
            {
                context.Warning($"Error encountered during native GKE workflow pod deletion: {ex.Message}");
            }
        }
    }
}
