using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using GitHub.DistributedTask.Pipelines.ContextData;
using GitHub.DistributedTask.WebApi;
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using GitHub.Runner.Worker.Handlers;
using GitHub.Services.WebApi;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json.Linq;

namespace GitHub.Runner.Worker.Container.ContainerHooks
{
    [ServiceLocator(Default = typeof(ContainerHookManager))]
    public interface IContainerHookManager : IRunnerService
    {
        Task PrepareJobAsync(IExecutionContext context, List<ContainerInfo> containers);
        Task RunContainerStepAsync(IExecutionContext context, ContainerInfo container, string dockerFile);
        Task RunScriptStepAsync(IExecutionContext context, ContainerInfo container, string workingDirectory, string fileName, string arguments, IDictionary<string, string> environment, string prependPath);
        Task CleanupJobAsync(IExecutionContext context, List<ContainerInfo> containers);
        string GetContainerHookData();
    }

    public class ContainerHookManager : RunnerService, IContainerHookManager
    {
        private const string ResponseFolderName = "_runner_hook_responses";
        private string HookScriptPath;

        public override void Initialize(IHostContext hostContext)
        {
            base.Initialize(hostContext);
            HookScriptPath = $"{Environment.GetEnvironmentVariable(Constants.Hooks.ContainerHooksPath)}";
        }

        public async Task PrepareJobAsync(IExecutionContext context, List<ContainerInfo> containers)
        {
            Trace.Entering();
            if (FeatureManager.IsNoSharedVolumeEnabled())
            {
                await PrepareJobNativeAsync(context, containers);
                return;
            }

            var jobContainer = containers.Where(c => c.IsJobContainer).SingleOrDefault();
            var serviceContainers = containers.Where(c => !c.IsJobContainer).ToList();

            var input = new HookInput
            {
                Command = HookCommand.PrepareJob,
                ResponseFile = GenerateResponsePath(),
                Args = new PrepareJobArgs
                {
                    Container = jobContainer?.GetHookContainer(),
                    Services = serviceContainers.Select(c => c.GetHookContainer()).ToList(),
                }
            };

            var prependPath = GetPrependPath(context);
            var response = await ExecuteHookScript<PrepareJobResponse>(context, input, ActionRunStage.Pre, prependPath);
            if (jobContainer != null)
            {
                jobContainer.IsAlpine = response.IsAlpine.Value;
            }
            SaveHookState(context, response.State, input);
            UpdateJobContext(context, jobContainer, serviceContainers, response);
        }

        public async Task RunContainerStepAsync(IExecutionContext context, ContainerInfo container, string dockerFile)
        {
            Trace.Entering();
            var hookState = context.Global.ContainerHookState;
            var containerStepArgs = new ContainerStepArgs(container);
            if (!string.IsNullOrEmpty(dockerFile))
            {
                containerStepArgs.Dockerfile = dockerFile;
                containerStepArgs.Image = null;
            }
            var input = new HookInput
            {
                Args = containerStepArgs,
                Command = HookCommand.RunContainerStep,
                ResponseFile = GenerateResponsePath(),
                State = hookState
            };

            var prependPath = GetPrependPath(context);
            var response = await ExecuteHookScript<HookResponse>(context, input, ActionRunStage.Pre, prependPath);
            if (response == null)
            {
                return;
            }
            SaveHookState(context, response.State, input);
        }

        public async Task RunScriptStepAsync(IExecutionContext context, ContainerInfo container, string workingDirectory, string entryPoint, string entryPointArgs, IDictionary<string, string> environmentVariables, string prependPath)
        {
            Trace.Entering();
            var input = new HookInput
            {
                Command = HookCommand.RunScriptStep,
                ResponseFile = GenerateResponsePath(),
                Args = new ScriptStepArgs
                {
                    EntryPointArgs = entryPointArgs.Split(' ').Select(arg => arg.Trim()),
                    EntryPoint = entryPoint,
                    EnvironmentVariables = environmentVariables,
                    PrependPath = context.Global.PrependPath.Reverse<string>(),
                    WorkingDirectory = workingDirectory,
                },
                State = context.Global.ContainerHookState
            };

            var response = await ExecuteHookScript<HookResponse>(context, input, ActionRunStage.Pre, prependPath);

            if (response == null)
            {
                return;
            }
            SaveHookState(context, response.State, input);
        }

        public async Task CleanupJobAsync(IExecutionContext context, List<ContainerInfo> containers)
        {
            Trace.Entering();
            if (FeatureManager.IsNoSharedVolumeEnabled())
            {
                await CleanupJobNativeAsync(context, containers);
                return;
            }

            var input = new HookInput
            {
                Command = HookCommand.CleanupJob,
                ResponseFile = GenerateResponsePath(),
                Args = new CleanupJobArgs(),
                State = context.Global.ContainerHookState
            };
            var prependPath = GetPrependPath(context);
            await ExecuteHookScript<HookResponse>(context, input, ActionRunStage.Pre, prependPath);
        }

        public string GetContainerHookData()
        {
            return JsonUtility.ToString(new { HookScriptPath });
        }

        private async Task<T> ExecuteHookScript<T>(IExecutionContext context, HookInput input, ActionRunStage stage, string prependPath) where T : HookResponse
        {
            try
            {
                ValidateHookExecutable();
                context.StepTelemetry.ContainerHookData = GetContainerHookData();
                var scriptDirectory = Path.GetDirectoryName(HookScriptPath);
                var stepHost = HostContext.CreateService<IDefaultStepHost>();

                Dictionary<string, string> inputs = new()
                {
                    ["standardInInput"] = JsonUtility.ToString(input),
                    ["path"] = HookScriptPath,
                    ["shell"] = HostContext.GetDefaultShellForScript(HookScriptPath, prependPath)
                };
                var handlerFactory = HostContext.GetService<IHandlerFactory>();
                var handler = handlerFactory.Create(
                                context,
                                null,
                                stepHost,
                                new ScriptActionExecutionData(),
                                inputs,
                                environment: new Dictionary<string, string>(VarUtil.EnvironmentVariableKeyComparer),
                                context.Global.Variables,
                                actionDirectory: scriptDirectory,
                                localActionContainerSetupSteps: null) as ScriptHandler;
                handler.PrepareExecution(stage);

                IOUtil.CreateEmptyFile(input.ResponseFile);
                await handler.RunAsync(stage);
                if (handler.ExecutionContext.Result == TaskResult.Failed)
                {
                    throw new Exception($"The hook script at '{HookScriptPath}' running command '{input.Command}' did not execute successfully");
                }
                var response = GetResponse<T>(input);
                return response;
            }
            catch (Exception ex)
            {
                throw new Exception($"Executing the custom container implementation failed. Please contact your self hosted runner administrator.", ex);
            }
        }

        private string GenerateResponsePath() => Path.Combine(HostContext.GetDirectory(WellKnownDirectory.Temp), ResponseFolderName, $"{Guid.NewGuid()}.json");

        private static string GetPrependPath(IExecutionContext context) => string.Join(Path.PathSeparator.ToString(), context.Global.PrependPath.Reverse<string>());

        private void ValidateHookExecutable()
        {
            if (!string.IsNullOrEmpty(HookScriptPath) && !File.Exists(HookScriptPath))
            {
                throw new FileNotFoundException($"File not found at '{HookScriptPath}'. Set {Constants.Hooks.ContainerHooksPath} to the path of an existing file.");
            }

            var supportedHookExtensions = new string[] { ".js", ".sh", ".ps1" };
            if (!supportedHookExtensions.Any(extension => HookScriptPath.EndsWith(extension)))
            {
                throw new ArgumentOutOfRangeException($"Invalid file extension at '{HookScriptPath}'. {Constants.Hooks.ContainerHooksPath} must be a path to a file with one of the following extensions: {string.Join(", ", supportedHookExtensions)}");
            }
        }

        private T GetResponse<T>(HookInput input) where T : HookResponse
        {
            if (!File.Exists(input.ResponseFile))
            {
                Trace.Info($"Response file for the hook script at '{HookScriptPath}' running command '{input.Command}' not found.");
                if (input.Args.IsRequireAlpineInResponse())
                {
                    throw new Exception($"Response file is required but not found for the hook script at '{HookScriptPath}' running command '{input.Command}'");
                }
                return null;
            }

            T response = IOUtil.LoadObject<T>(input.ResponseFile);
            Trace.Info($"Response file for the hook script at '{HookScriptPath}' running command '{input.Command}' was processed successfully");
            IOUtil.DeleteFile(input.ResponseFile);
            Trace.Info($"Response file for the hook script at '{HookScriptPath}' running command '{input.Command}' was deleted successfully");
            if (response == null && input.Args.IsRequireAlpineInResponse())
            {
                throw new Exception($"Response file could not be read at '{HookScriptPath}' running command '{input.Command}'");
            }
            response?.Validate(input);
            return response;
        }

        private void SaveHookState(IExecutionContext context, JObject hookState, HookInput input)
        {
            if (hookState == null)
            {
                Trace.Info($"No 'state' property found in response file for '{input.Command}'. Global variable for 'ContainerHookState' will not be updated.");
                return;
            }
            context.Global.ContainerHookState = hookState;
            Trace.Info($"Global variable 'ContainerHookState' updated successfully for '{input.Command}' with data found in 'state' property of the response file.");
        }

        private void UpdateJobContext(IExecutionContext context, ContainerInfo jobContainer, List<ContainerInfo> serviceContainers, PrepareJobResponse response)
        {
            if (response.Context == null)
            {
                Trace.Info($"The response file does not contain a context. The fields 'jobContext.Container' and 'jobContext.Services' will not be set.");
                return;
            }

            var containerId = response.Context.Container?.Id;
            if (containerId != null)
            {
                context.JobContext.Container["id"] = new StringContextData(containerId);
                jobContainer.ContainerId = containerId;
            }

            var containerIP = response.Context.Container?.PodIP;
            if (containerIP != null)
            {
                jobContainer.ContainerIP = containerIP;
            }

            var containerNetwork = response.Context.Container?.Network;
            if (containerNetwork != null)
            {
                context.JobContext.Container["network"] = new StringContextData(containerNetwork);
                jobContainer.ContainerNetwork = containerNetwork;
            }

            for (var i = 0; i < response.Context.Services.Count; i++)
            {
                var responseContainerInfo = response.Context.Services[i];
                var globalContainerInfo = serviceContainers[i];
                globalContainerInfo.ContainerId = responseContainerInfo.Id;
                globalContainerInfo.ContainerIP = responseContainerInfo.PodIP;
                globalContainerInfo.ContainerNetwork = responseContainerInfo.Network;

                var service = new DictionaryContextData()
                {
                    ["id"] = new StringContextData(responseContainerInfo.Id),
                    ["ports"] = new DictionaryContextData(),
                    ["network"] = new StringContextData(responseContainerInfo.Network)
                };

                globalContainerInfo.AddPortMappings(responseContainerInfo.Ports);
                foreach (var portMapping in responseContainerInfo.Ports)
                {
                    (service["ports"] as DictionaryContextData)[portMapping.Key] = new StringContextData(portMapping.Value);
                }
                context.JobContext.Services[globalContainerInfo.ContainerNetworkAlias] = service;
            }
        }

        private async Task PrepareJobNativeAsync(IExecutionContext context, List<ContainerInfo> containers)
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

        private async Task CleanupJobNativeAsync(IExecutionContext context, List<ContainerInfo> containers)
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
