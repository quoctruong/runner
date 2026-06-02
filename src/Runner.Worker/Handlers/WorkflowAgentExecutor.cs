using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GitHub.Runner.Common;
using GitHub.Runner.Worker.Container;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GitHub.Runner.Worker.Handlers
{
    public static class WorkflowAgentExecutor
    {
        public static async Task<int> ExecuteAsync(
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
    }
}
