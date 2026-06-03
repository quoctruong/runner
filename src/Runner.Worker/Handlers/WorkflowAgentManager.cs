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
using GitHub.Runner.Worker.Protos;
using Grpc.Core;
using Grpc.Net.Client;

namespace GitHub.Runner.Worker.Handlers
{
    public class WorkflowAgentManager : RunnerService, IWorkflowAgentManager
    {
        private WorkflowAgent.WorkflowAgentClient GetGrpcClient(string podIP)
        {
            // Establish gRPC channel via private GKE pod-to-pod routing over HTTP/2 (Port 50051)
            var channel = GrpcChannel.ForAddress($"http://{podIP}:50051", new GrpcChannelOptions
            {
                Credentials = ChannelCredentials.Insecure,
            });
            return new WorkflowAgent.WorkflowAgentClient(channel);
        }

        private Metadata GetHeaders()
        {
            var headers = new Metadata();
            var token = Environment.GetEnvironmentVariable(Constants.Variables.Actions.SecurityToken);
            if (!string.IsNullOrEmpty(token))
            {
                headers.Add("x-actions-runner-token", token);
            }
            return headers;
        }

        public async Task WriteFileAsync(string podIP, string path, string content)
        {
            var client = GetGrpcClient(podIP);
            var request = new WriteFileRequest
            {
                Path = path,
                Content = content
            };
            await client.WriteFileAsync(request, headers: GetHeaders());
        }

        public async Task<string> ReadFileAsync(string podIP, string path)
        {
            var client = GetGrpcClient(podIP);
            var request = new ReadFileRequest
            {
                Path = path
            };
            var response = await client.ReadFileAsync(request, headers: GetHeaders());
            return response.Content;
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

            var client = GetGrpcClient(podIP);
            var request = new ExecuteRequest
            {
                Command = fileName,
                Arguments = arguments,
                WorkingDirectory = workingDirectory,
                Stdin = standardInInput ?? string.Empty,
                PrependPath = prependPath ?? string.Empty
            };

            foreach (var env in environment)
            {
                request.Environment.Add(env.Key, env.Value ?? string.Empty);
            }

            using (var call = client.Execute(request, headers: GetHeaders(), cancellationToken: cancellationToken))
            {
                while (await call.ResponseStream.MoveNext(cancellationToken))
                {
                    var response = call.ResponseStream.Current;
                    if (response.Stream == ExecuteResponse.Types.StreamType.Stdout)
                    {
                        var data = response.Data;
                        if (data != null)
                        {
                            onOutput?.Invoke(data.TrimEnd('\r', '\n'));
                        }
                    }
                    else if (response.Stream == ExecuteResponse.Types.StreamType.Stderr)
                    {
                        var data = response.Data;
                        if (data != null)
                        {
                            onError?.Invoke(data.TrimEnd('\r', '\n'));
                        }
                    }
                    else if (response.Stream == ExecuteResponse.Types.StreamType.ExitCode)
                    {
                        return response.ExitCode;
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
