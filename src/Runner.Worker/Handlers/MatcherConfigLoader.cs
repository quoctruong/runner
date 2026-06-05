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
    internal static class MatcherConfigLoader
    {
        public static IssueMatchersConfig Load(IExecutionContext context, string file, ContainerInfo container, IWorkflowAgentManager workflowAgentManager)
        {
            file = file?.Trim();
            ArgUtil.NotNull(container, nameof(container));

            // Root the container path if it is relative
            string containerPath = file;
            if (!Path.IsPathRooted(containerPath))
            {
                var githubContext = context.ExpressionValues["github"] as GitHubContext;
                ArgUtil.NotNull(githubContext, nameof(githubContext));
                var workspace = githubContext["workspace"].ToString();
                ArgUtil.NotNullOrEmpty(workspace, "workspace");

                var hostFile = Path.Combine(workspace, containerPath);
                containerPath = container.TranslateToContainerPath(hostFile);
            }

            context.Debug($"[MatcherConfigLoader] Retrieving matcher config from workflow agent: {containerPath}");

            string tempFile = null;
            try
            {
                var content = workflowAgentManager.ReadFileAsync(container.ContainerIP, containerPath).GetAwaiter().GetResult();
                tempFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".json");
                File.WriteAllText(tempFile, content);

                return IOUtil.LoadObject<IssueMatchersConfig>(tempFile);
            }
            finally
            {
                if (tempFile != null && File.Exists(tempFile))
                {
                    try
                    {
                        File.Delete(tempFile);
                    }
                    catch
                    {
                        // Ignore cleanup errors
                    }
                }
            }
        }
    }
}
