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

        public Task<int> ExecuteAsync(
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
            throw new NotImplementedException();
        }

        public Task SyncWebhookPayloadAsync(IExecutionContext context, string localFilePath, string content)
        {
            throw new NotImplementedException();
        }

        public void InitializeFileCommand(IExecutionContext context, ContainerInfo container, string hostPath, string contextName)
        {
            throw new NotImplementedException();
        }

        public Task SyncFileCommandsFromWorkflowPodAsync(IExecutionContext context, ContainerInfo container, string fileCommandDirectory, string fileSuffix, IEnumerable<IFileCommandExtension> commandExtensions)
        {
            throw new NotImplementedException();
        }

        public Task SyncFileToWorkflowPodAsync(IExecutionContext context, string hostPath)
        {
            throw new NotImplementedException();
        }

        public Task SyncDirectoryToWorkflowPodAsync(IExecutionContext context, string hostDirectory)
        {
            throw new NotImplementedException();
        }
    }
}
