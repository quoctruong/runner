using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GitHub.Runner.Worker.Handlers
{
    public static class WorkflowAgentHelper
    {

        public static Func<string, string, Task<string>> ReadFileDelegate { get; set; }
        public static Func<string, string, string, Task> WriteFileDelegate { get; set; }

        public static async Task WriteFileAsync(string podIP, string path, string content)
        {
            if (WriteFileDelegate != null)
            {
                await WriteFileDelegate(podIP, path, content);
                return;
            }

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

        public static async Task<string> ReadFileAsync(string podIP, string path)
        {
            if (ReadFileDelegate != null)
            {
                return await ReadFileDelegate(podIP, path);
            }

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
    }
}
