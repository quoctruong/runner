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
        private const string CertsDirectory = "/etc/certs";
        private const string CaCertPath = "/etc/certs/ca.crt";
        private const string ClientCertPath = "/etc/certs/client.crt";
        private const string ClientKeyPath = "/etc/certs/client.key";
        private const int ChunkSize = 64 * 1024;

        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _syncedDirectories = new(StringComparer.OrdinalIgnoreCase);
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, GrpcChannel> _channels = new(StringComparer.OrdinalIgnoreCase);
        private System.Security.Cryptography.X509Certificates.X509Certificate2 _clientCert;
        private System.Security.Cryptography.X509Certificates.X509Certificate2 _caCert;
        private readonly object _certLock = new object();

        private WorkflowAgent.WorkflowAgentClient GetGrpcClient(string podIP)
        {
            var channel = _channels.GetOrAdd(podIP, CreateGrpcChannel);
            return new WorkflowAgent.WorkflowAgentClient(channel);
        }

        private GrpcChannel CreateGrpcChannel(string ip)
        {
            var agentPort = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_WORKFLOW_AGENT_PORT") ?? "50051";

            EnsureCertificatesLoaded();

            var handler = new SocketsHttpHandler
            {
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
                KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always
            };

            try
            {
                var targetHost = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_POD_NAME")?.Replace("runner-", "workflow-agent-") ?? "workflow-agent";
                handler.SslOptions = new System.Net.Security.SslClientAuthenticationOptions
                {
                    TargetHost = targetHost,
                    ClientCertificates = new System.Security.Cryptography.X509Certificates.X509Certificate2Collection { _clientCert },
                    RemoteCertificateValidationCallback = ValidateServerCertificate
                };

                return GrpcChannel.ForAddress($"https://{ip}:{agentPort}", new GrpcChannelOptions
                {
                    HttpHandler = handler
                });
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to initialize secure mTLS connection: {ex.Message}", ex);
            }
        }

        private void EnsureCertificatesLoaded()
        {
            if (!Directory.Exists(CertsDirectory) || !File.Exists(CaCertPath) || !File.Exists(ClientCertPath) || !File.Exists(ClientKeyPath))
            {
                throw new InvalidOperationException($"mTLS certificates not found in '{CertsDirectory}'. Transport encryption is strictly required.");
            }

            lock (_certLock)
            {
                if (_clientCert == null)
                {
                    _clientCert = System.Security.Cryptography.X509Certificates.X509Certificate2.CreateFromPemFile(
                        ClientCertPath,
                        ClientKeyPath
                    );
                }
                if (_caCert == null)
                {
                    _caCert = new System.Security.Cryptography.X509Certificates.X509Certificate2(CaCertPath);
                }
            }
        }

        private bool ValidateServerCertificate(object sender, System.Security.Cryptography.X509Certificates.X509Certificate certificate, System.Security.Cryptography.X509Certificates.X509Chain chain, System.Net.Security.SslPolicyErrors errors)
        {
            if (certificate == null)
            {
                Trace.Info("mTLS server certificate verification failed: server certificate is null.");
                return false;
            }

            Trace.Info($"mTLS validating server certificate '{certificate.Subject}' against CA '{_caCert.Subject}'...");
            var chainPolicy = new System.Security.Cryptography.X509Certificates.X509ChainPolicy
            {
                RevocationMode = System.Security.Cryptography.X509Certificates.X509RevocationMode.NoCheck,
                VerificationFlags = System.Security.Cryptography.X509Certificates.X509VerificationFlags.AllowUnknownCertificateAuthority
            };
            chainPolicy.ExtraStore.Add(_caCert);

            using (var x509Chain = new System.Security.Cryptography.X509Certificates.X509Chain { ChainPolicy = chainPolicy })
            using (var targetCert = new System.Security.Cryptography.X509Certificates.X509Certificate2(certificate))
            {
                bool isValid = x509Chain.Build(targetCert);
                if (!isValid)
                {
                    Trace.Info("mTLS server certificate verification failed: X509Chain build failed.");
                    return false;
                }

                foreach (var element in x509Chain.ChainElements)
                {
                    if (element.Certificate.Thumbprint == _caCert.Thumbprint)
                    {
                        Trace.Info("mTLS server certificate verification succeeded: resolved to trusted CA.");
                        return true;
                    }
                }
                Trace.Info("mTLS server certificate verification failed: certificate chain did not resolve to the trusted CA.");
                return false;
            }
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

        public async Task WriteFileAsync(string podIP, string path, Stream content)
        {
            var client = GetGrpcClient(podIP);
            using (var call = client.WriteFile(headers: GetHeaders()))
            {
                await call.RequestStream.WriteAsync(new WriteFileRequest { Path = path });

                var buffer = new byte[ChunkSize];
                int bytesRead;
                while ((bytesRead = await content.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    await call.RequestStream.WriteAsync(new WriteFileRequest { Chunk = Google.Protobuf.ByteString.CopyFrom(buffer, 0, bytesRead) });
                }

                await call.RequestStream.CompleteAsync();
                var response = await call.ResponseAsync;
                if (response == null || !response.Success)
                {
                    throw new InvalidOperationException("WriteFile failed: server returned success = false");
                }
            }
        }

        public async Task ReadFileAsync(string podIP, string path, Stream outputStream)
        {
            var client = GetGrpcClient(podIP);
            var request = new ReadFileRequest
            {
                Path = path
            };
            using (var call = client.ReadFile(request, headers: GetHeaders()))
            {
                while (await call.ResponseStream.MoveNext(default))
                {
                    var chunk = call.ResponseStream.Current.Chunk;
                    if (chunk != null)
                    {
                        chunk.WriteTo(outputStream);
                    }
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
                // Protobuf MapField throws ArgumentNullException if null values are inserted.
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
                            // The Go gRPC server streams complete lines ending in \n.
                            // Stripping trailing newlines is required to match standard .NET Process.OutputDataReceived behavior.
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
                    var fileName = string.IsNullOrEmpty(localFilePath) ? "event.json" : Path.GetFileName(localFilePath);
                    var containerPath = $"/github/workflow/{fileName}";
                    context.Debug($"Syncing event payload to workflow pod: {containerPath}");
                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(content ?? string.Empty)))
                    {
                        await WriteFileAsync(podIP, containerPath, stream);
                    }
                }
                catch (Exception ex)
                {
                    context.Warning($"Failed to upload event payload to workflow pod: {ex.Message}");
                }
            }
        }

        public void InitializeFileCommand(IExecutionContext context, ContainerInfo container, string hostPath, string contextName)
        {
            throw new NotImplementedException();
        }

        public Task SyncFileCommandsFromWorkflowPodAsync(IExecutionContext context, ContainerInfo container, string fileCommandDirectory, string fileSuffix, IEnumerable<IFileCommandExtension> commandExtensions)
        {
            throw new NotImplementedException();
        }

        public async Task SyncFileToWorkflowPodAsync(IExecutionContext context, string hostPath)
        {
            if (!FeatureManager.IsNoSharedVolumeEnabled()) return;

            var podIP = context.Global.Container?.ContainerIP;
            if (string.IsNullOrEmpty(podIP) || string.IsNullOrEmpty(hostPath) || !File.Exists(hostPath)) return;

            context.Debug($"Syncing file to workflow pod: {hostPath}");
            using (var stream = File.OpenRead(hostPath))
            {
                await WriteFileAsync(podIP, hostPath, stream);
            }
        }

        public async Task SyncDirectoryToWorkflowPodAsync(IExecutionContext context, string hostDirectory)
        {
            if (!FeatureManager.IsNoSharedVolumeEnabled()) return;

            var podIP = context.Global.Container?.ContainerIP;
            if (string.IsNullOrEmpty(podIP) || string.IsNullOrEmpty(hostDirectory) || !Directory.Exists(hostDirectory)) return;

            var normalizedDirectory = Path.GetFullPath(hostDirectory);
            var actionsDir = Path.GetFullPath(HostContext.GetDirectory(WellKnownDirectory.Actions));
            bool isImmutableAction = normalizedDirectory.StartsWith(actionsDir, StringComparison.OrdinalIgnoreCase);

            if (isImmutableAction)
            {
                if (!_syncedDirectories.TryAdd(normalizedDirectory, 0))
                {
                    context.Debug($"Directory '{normalizedDirectory}' has already been synced to workflow pod.");
                    return;
                }
            }

            var files = Directory.GetFiles(hostDirectory, "*", SearchOption.AllDirectories);
            context.Debug($"Syncing {files.Length} files under '{hostDirectory}' to workflow pod IP '{podIP}'...");

            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = 16
            };

            await Parallel.ForEachAsync(files, parallelOptions, async (filePath, token) =>
            {
                var resolvedPath = context.Global.Container?.TranslateToContainerPath(filePath) ?? filePath;
                try
                {
                    using (var stream = File.OpenRead(filePath))
                    {
                        await WriteFileAsync(podIP, resolvedPath, stream);
                    }
                }
                catch (Exception ex)
                {
                    context.Warning($"Failed to sync file '{filePath}' to workflow pod: {ex.Message}");
                }
            });

            context.Debug($"Finished syncing directory '{hostDirectory}'.");
        }
    }
}
