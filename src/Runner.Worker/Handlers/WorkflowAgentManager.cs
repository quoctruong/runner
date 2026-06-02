using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GitHub.Runner.Common;
using GitHub.Runner.Worker;
using GitHub.Runner.Worker.Container;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GitHub.Runner.Worker.Handlers
{
    public class WorkflowAgentManager : RunnerService, IWorkflowAgentManager
    {
        public async Task WriteFileAsync(string podIP, string path, string content)
        {
            var requestPayload = new
            {
                path = path,
                content = content
            };
            var json = JsonConvert.SerializeObject(requestPayload);

            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
            };

            using (var client = new HttpClient(handler))
            {
                var stringContent = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await client.PostAsync($"http://{podIP}:8080/write-file", stringContent);
                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    throw new HttpRequestException($"Failed to write file: {(int)response.StatusCode} ({response.ReasonPhrase}). Response Body: {errorBody}");
                }
            }
        }

        public async Task<string> ReadFileAsync(string podIP, string path)
        {
            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
            };

            using (var client = new HttpClient(handler))
            {
                var response = await client.GetAsync($"http://{podIP}:8080/read-file?path={Uri.EscapeDataString(path)}");
                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    throw new HttpRequestException($"Failed to read file: {(int)response.StatusCode} ({response.ReasonPhrase}). Response Body: {errorBody}");
                }
                return await response.Content.ReadAsStringAsync();
            }
        }

        public async Task<int> ExecuteAsync(
            IExecutionContext context,
            ContainerInfo container,
            string workingDirectory,
            string fileName,
            string arguments,
            IDictionary<string, string> environment,
            string standardInInput,
            string prependPath,
            Action<string> onOutput,
            Action<string> onError,
            CancellationToken cancellationToken)
        {
            var podIP = container?.ContainerIP;
            if (string.IsNullOrEmpty(podIP))
            {
                throw new InvalidOperationException("Workflow pod IP is not available.");
            }
            context.Debug($"Resolved workflow pod IP: {podIP}");

            var execEnvironment = new Dictionary<string, string>(environment, StringComparer.OrdinalIgnoreCase);
            if (!string.IsNullOrEmpty(prependPath))
            {
                context.Debug($"Prepending path to workflow agent execution: {prependPath}");
                execEnvironment["ACTIONS_RUNNER_PREPEND_PATH"] = prependPath;
            }

            var execRequest = new
            {
                command = fileName,
                arguments = arguments,
                env = execEnvironment,
                workingDir = workingDirectory,
                stdin = standardInInput ?? string.Empty
            };
            var json = JsonConvert.SerializeObject(execRequest);

            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
            };

            using (var client = new HttpClient(handler))
            {
                client.Timeout = Timeout.InfiniteTimeSpan;
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var request = new HttpRequestMessage(HttpMethod.Post, $"http://{podIP}:8080/execute") { Content = content };

                using (var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken))
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        var errorBody = await response.Content.ReadAsStringAsync();
                        throw new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase}). Response Body: {errorBody}");
                    }
                    using (var stream = await response.Content.ReadAsStreamAsync())
                    using (var reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = await reader.ReadLineAsync();
                            if (string.IsNullOrEmpty(line)) continue;

                            var chunk = JObject.Parse(line);
                            var streamName = chunk["stream"]?.ToString();
                            var data = chunk["data"]?.ToString();
                            var exitCodeToken = chunk["exitCode"];

                            if (exitCodeToken != null)
                            {
                                return exitCodeToken.Value<int>();
                            }

                            if (data != null)
                            {
                                data = data.TrimEnd('\r', '\n');
                                if (string.Equals(streamName, "stdout", StringComparison.OrdinalIgnoreCase))
                                {
                                    onOutput?.Invoke(data);
                                }
                                else if (string.Equals(streamName, "stderr", StringComparison.OrdinalIgnoreCase))
                                {
                                    onError?.Invoke(data);
                                }
                            }
                        }
                    }
                }
            }
            throw new InvalidOperationException("Workflow agent terminated stream connection without returning exit code.");
        }

        public async Task SyncWebhookPayloadAsync(IExecutionContext context, string localFilePath, string content)
        {
            if (!FeatureManager.IsNoSharedVolumeEnabled()) return;

            var podIP = context.Global.Container?.ContainerIP;
            if (!string.IsNullOrEmpty(podIP))
            {
                try
                {
                    var containerPath = "/github/workflow/event.json";
                    context.Debug($"Syncing event payload to workflow pod: {containerPath}");
                    await WriteFileAsync(podIP, containerPath, content);
                }
                catch (Exception ex)
                {
                    context.Warning($"Failed to upload event payload to workflow pod: {ex.Message}");
                }
            }
        }

        public void InitializeFileCommand(IExecutionContext context, ContainerInfo container, string hostPath, string contextName)
        {
            if (!FeatureManager.IsNoSharedVolumeEnabled() || container == null) return;

            var containerPath = container.TranslateToContainerPath(hostPath);
            var podIP = container.ContainerIP;
            if (!string.IsNullOrEmpty(podIP))
            {
                try
                {
                    context.Debug($"Initializing empty file command {contextName} on workflow pod: {containerPath}");
                    WriteFileAsync(podIP, containerPath, string.Empty).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    context.Debug($"Failed to initialize empty file command {contextName} on workflow pod: {ex.Message}");
                }
            }
        }

        public async Task SyncFileCommandsFromWorkflowPodAsync(
            IExecutionContext context,
            ContainerInfo container,
            string fileCommandDirectory,
            string fileSuffix,
            IEnumerable<IFileCommandExtension> commandExtensions)
        {
            if (!FeatureManager.IsNoSharedVolumeEnabled() || container == null) return;

            var podName = container.ContainerId;
            if (string.IsNullOrEmpty(podName)) return;

            var podIP = container.ContainerIP;
            if (string.IsNullOrEmpty(podIP))
            {
                context.Warning("Workflow pod IP is not available for file command sync.");
                return;
            }

            foreach (var fileCommand in commandExtensions)
            {
                var localPath = Path.Combine(fileCommandDirectory, fileCommand.FilePrefix + fileSuffix);
                var containerPath = container.TranslateToContainerPath(localPath);

                context.Debug($"Syncing file command {fileCommand.ContextName} from workflow pod: {containerPath} -> {localPath}");
                try
                {
                    var content = await ReadFileAsync(podIP, containerPath);
                    if (!string.IsNullOrEmpty(content))
                    {
                        File.WriteAllText(localPath, content, Encoding.UTF8);
                    }
                }
                catch (Exception ex)
                {
                    // File might not exist if the step didn't write to it, which is normal
                    context.Debug($"Failed to sync file command {fileCommand.ContextName}: {ex.Message}");
                }
            }
        }

        public async Task SyncFileToWorkflowPodAsync(IExecutionContext context, string hostPath)
        {
            if (!FeatureManager.IsNoSharedVolumeEnabled()) return;

            var podIP = context.Global.Container?.ContainerIP;
            if (!string.IsNullOrEmpty(podIP) && context.Global.Container != null)
            {
                var resolvedScriptPath = context.Global.Container.TranslateToContainerPath(hostPath);
                var scriptContent = File.ReadAllText(hostPath);
                await WriteFileAsync(podIP, resolvedScriptPath, scriptContent);
            }
        }
    }
}
