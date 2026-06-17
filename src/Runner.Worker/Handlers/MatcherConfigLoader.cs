using System;
using System.IO;
using GitHub.DistributedTask.Pipelines.ContextData;
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using GitHub.Runner.Worker;
using GitHub.Runner.Worker.Container;

namespace GitHub.Runner.Worker.Handlers
{
    /// <summary>
    /// Utility class to load GitHub Action problem matcher configurations.
    /// In native Kubernetes pod orchestration, the workspace folder is located on the remote pod's volume mount.
    /// Since the runner host cannot read the matcher JSON config file directly from local disk, this class
    /// fetches the file content over gRPC via <see cref="IWorkflowAgentManager"/> and parses it locally.
    /// </summary>
    internal static class MatcherConfigLoader
    {
        /// <summary>
        /// Retrieves the problem matcher configuration from the Kubernetes pod container and loads it.
        /// </summary>
        /// <param name="context">The active step execution context.</param>
        /// <param name="file">The workspace-relative or rooted container path to the problem matcher JSON file.</param>
        /// <param name="container">The target Kubernetes container descriptor.</param>
        /// <param name="workflowAgentManager">The gRPC manager client connected to the remote workflow pod.</param>
        /// <returns>The parsed <see cref="IssueMatchersConfig"/> containing regular expression annotations.</returns>
        public static IssueMatchersConfig Load(IExecutionContext context, string file, ContainerInfo container, IWorkflowAgentManager workflowAgentManager)
        {
            ArgUtil.NotNullOrEmpty(file, nameof(file));
            file = file.Trim();
            ArgUtil.NotNull(container, nameof(container));

            // Root the container path if it is relative
            string containerPath = file;
            if (!Path.IsPathRooted(containerPath))
            {
                var workspace = (context.ExpressionValues["github"] as GitHubContext)?["workspace"]?.ToString();
                ArgUtil.NotNullOrEmpty(workspace, "workspace");

                var hostFile = Path.Combine(workspace, containerPath);
                containerPath = container.TranslateToContainerPath(hostFile);
            }

            context.Debug($"[MatcherConfigLoader] Retrieving matcher config from workflow agent: {containerPath}");

            try
            {
                using var ms = new MemoryStream();
                workflowAgentManager.ReadFileAsync(container.ContainerIP, containerPath, ms).GetAwaiter().GetResult();
                var content = System.Text.Encoding.UTF8.GetString(ms.ToArray());
                return StringUtil.ConvertFromJson<IssueMatchersConfig>(content);
            }
            catch (Exception ex)
            {
                context.Error($"Failed to retrieve/deserialize matcher config from workflow agent: {ex.Message}");
                throw;
            }
        }
    }
}
