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
        private readonly HashSet<string> _syncedDirectories = new(StringComparer.OrdinalIgnoreCase);

        private WorkflowAgent.WorkflowAgentClient GetGrpcClient(string podIP)
        {
            var agentPort = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_WORKFLOW_AGENT_PORT") ?? "50051";
            GrpcChannel channel;

            if (!Directory.Exists("/etc/certs") || !File.Exists("/etc/certs/ca.crt") || !File.Exists("/etc/certs/client.crt") || !File.Exists("/etc/certs/client.key"))
            {
                throw new InvalidOperationException("mTLS certificates not found in '/etc/certs'. Transport encryption is strictly required.");
            }

            var handler = new SocketsHttpHandler
            {
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
                KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always
            };
            try
            {
                var clientCert = System.Security.Cryptography.X509Certificates.X509Certificate2.CreateFromPemFile(
                    "/etc/certs/client.crt",
                    "/etc/certs/client.key"
                );
                var caCert = new System.Security.Cryptography.X509Certificates.X509Certificate2("/etc/certs/ca.crt");

                handler.SslOptions = new System.Net.Security.SslClientAuthenticationOptions
                {
                    ClientCertificates = new System.Security.Cryptography.X509Certificates.X509Certificate2Collection { clientCert },
                    RemoteCertificateValidationCallback = (sender, certificate, chain, errors) =>
                    {
                        if (certificate == null)
                        {
                            Trace.Info("mTLS server certificate verification failed: server certificate is null.");
                            return false;
                        }

                        Trace.Info($"mTLS validating server certificate '{certificate.Subject}' against CA '{caCert.Subject}'...");
                        var chainPolicy = new System.Security.Cryptography.X509Certificates.X509ChainPolicy
                        {
                            RevocationMode = System.Security.Cryptography.X509Certificates.X509RevocationMode.NoCheck,
                            VerificationFlags = System.Security.Cryptography.X509Certificates.X509VerificationFlags.AllowUnknownCertificateAuthority
                        };
                        chainPolicy.ExtraStore.Add(caCert);

                        var x509Chain = new System.Security.Cryptography.X509Certificates.X509Chain
                        {
                            ChainPolicy = chainPolicy
                        };

                        var targetCert = new System.Security.Cryptography.X509Certificates.X509Certificate2(certificate);
                        bool isValid = x509Chain.Build(targetCert);
                        if (!isValid)
                        {
                            Trace.Info("mTLS server certificate verification failed: X509Chain build failed.");
                            return false;
                        }

                        foreach (var element in x509Chain.ChainElements)
                        {
                            if (element.Certificate.Thumbprint == caCert.Thumbprint)
                            {
                                Trace.Info("mTLS server certificate verification succeeded: resolved to trusted CA.");
                                return true;
                            }
                        }
                        Trace.Info("mTLS server certificate verification failed: certificate chain did not resolve to the trusted CA.");
                        return false;
                    }
                };

                channel = GrpcChannel.ForAddress($"https://{podIP}:{agentPort}", new GrpcChannelOptions
                {
                    HttpHandler = handler
                });
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to initialize secure mTLS connection: {ex.Message}", ex);
            }
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
            using (var call = client.WriteFile(headers: GetHeaders()))
            {
                await call.RequestStream.WriteAsync(new WriteFileRequest { Path = path });

                var bytes = Encoding.UTF8.GetBytes(content ?? string.Empty);
                const int chunkSize = 64 * 1024;
                for (int i = 0; i < bytes.Length; i += chunkSize)
                {
                    int length = Math.Min(chunkSize, bytes.Length - i);
                    var chunk = new byte[length];
                    Array.Copy(bytes, i, chunk, 0, length);
                    await call.RequestStream.WriteAsync(new WriteFileRequest { Chunk = Google.Protobuf.ByteString.CopyFrom(chunk) });
                }

                await call.RequestStream.CompleteAsync();
                var response = await call.ResponseAsync;
                if (response == null || !response.Success)
                {
                    throw new InvalidOperationException("WriteFile failed: server returned success = false");
                }
            }
        }

        public async Task<string> ReadFileAsync(string podIP, string path)
        {
            var client = GetGrpcClient(podIP);
            var request = new ReadFileRequest
            {
                Path = path
            };
            using (var call = client.ReadFile(request, headers: GetHeaders()))
            {
                using (var ms = new MemoryStream())
                {
                    while (await call.ResponseStream.MoveNext(default))
                    {
                        var chunk = call.ResponseStream.Current.Chunk;
                        if (chunk != null)
                        {
                            chunk.WriteTo(ms);
                        }
                    }
                    return Encoding.UTF8.GetString(ms.ToArray());
                }
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

        public async Task SyncDirectoryToWorkflowPodAsync(IExecutionContext context, string hostDirectory)
        {
            if (!FeatureManager.IsNoSharedVolumeEnabled())
            {
                context.Debug("Bypassing directory sync to workflow pod because shared volume feature is enabled (no-shared-volume is false).");
                return;
            }
            if (!Directory.Exists(hostDirectory))
            {
                context.Warning($"Directory sync skipped: host directory '{hostDirectory}' does not exist.");
                return;
            }

            var normalizedDirectory = Path.GetFullPath(hostDirectory);
            var actionsDir = Path.GetFullPath(HostContext.GetDirectory(WellKnownDirectory.Actions));

            // Only cache/skip sync if it's an immutable third-party action under the '_actions/' directory
            bool isImmutableAction = normalizedDirectory.StartsWith(actionsDir, StringComparison.OrdinalIgnoreCase);
            context.Debug($"SyncDirectoryToWorkflowPodAsync starting: hostDirectory='{hostDirectory}' (normalized='{normalizedDirectory}'), isImmutableAction={isImmutableAction}");

            if (isImmutableAction)
            {
                lock (_syncedDirectories)
                {
                    if (_syncedDirectories.Contains(normalizedDirectory))
                    {
                        context.Debug($"Directory {normalizedDirectory} has already been synced to the workflow pod. Skipping.");
                        return;
                    }
                    _syncedDirectories.Add(normalizedDirectory);
                }
            }

            var podIP = context.Global.Container?.ContainerIP;
            if (string.IsNullOrEmpty(podIP) || context.Global.Container == null)
            {
                context.Warning($"Directory sync aborted: Workflow pod IP or container is not available. podIP='{podIP ?? "null"}', containerExists={context.Global.Container != null}");
                return;
            }

            var files = Directory.GetFiles(hostDirectory, "*", SearchOption.AllDirectories);
            context.Debug($"Found {files.Length} files under '{hostDirectory}' to sync to workflow pod IP '{podIP}'.");

            var tasks = new List<Task>();
            foreach (var filePath in files)
            {
                var resolvedPath = context.Global.Container.TranslateToContainerPath(filePath);
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        context.Debug($"Syncing action file '{filePath}' -> '{resolvedPath}' on pod IP '{podIP}'...");
                        var content = File.ReadAllText(filePath);
                        await WriteFileAsync(podIP, resolvedPath, content);
                        context.Debug($"Successfully synced action file '{filePath}'.");
                    }
                    catch (Exception ex)
                    {
                        context.Warning($"Failed to sync action file {filePath} to workflow pod: {ex.Message}");
                    }
                }));
            }

            if (tasks.Count > 0)
            {
                context.Debug($"Syncing {tasks.Count} action files in parallel to workflow pod...");
                await Task.WhenAll(tasks);
                context.Debug($"Successfully finished syncing all {tasks.Count} action files in parallel.");
            }
            else
            {
                context.Debug($"No files found to sync in directory '{hostDirectory}'.");
            }
        }
    }
}
