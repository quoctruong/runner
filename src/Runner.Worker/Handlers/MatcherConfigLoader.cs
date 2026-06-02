using System;
using System.IO;
using GitHub.DistributedTask.Pipelines.ContextData;
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using GitHub.Runner.Worker.Container;

namespace GitHub.Runner.Worker.Handlers
{
    internal static class MatcherConfigLoader
    {
        public static IssueMatchersConfig Load(IExecutionContext context, string file, ContainerInfo container)
        {
            file = file?.Trim();
            var noSharedVolume = string.Equals(Environment.GetEnvironmentVariable(Constants.Variables.Actions.NoSharedVolume), "true", StringComparison.OrdinalIgnoreCase);

            // Translate file path back from container path to check if it exists on host
            string hostPath = file;
            if (container != null)
            {
                hostPath = container.TranslateToHostPath(hostPath);
            }

            // Root the host path
            if (!Path.IsPathRooted(hostPath))
            {
                var githubContext = context.ExpressionValues["github"] as GitHubContext;
                ArgUtil.NotNull(githubContext, nameof(githubContext));
                var workspace = githubContext["workspace"].ToString();
                ArgUtil.NotNullOrEmpty(workspace, "workspace");

                hostPath = Path.Combine(workspace, hostPath);
            }

            context.Debug($"[MatcherConfigLoader] Load file: '{file}'");
            context.Debug($"[MatcherConfigLoader] translated hostPath: '{hostPath}', exists: {File.Exists(hostPath)}");

            // If it exists locally on the host runner, load it directly
            if (File.Exists(hostPath))
            {
                context.Debug($"[MatcherConfigLoader] File exists on host. Loading locally.");
                return IOUtil.LoadObject<IssueMatchersConfig>(hostPath);
            }

            if (noSharedVolume && container != null)
            {
                // Root the path to get containerPath
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

                // Retrieve file content from workflow agent on the workflow pod
                string tempFile = null;
                try
                {
                    var content = WorkflowAgentHelper.ReadFileAsync(container.ContainerIP, containerPath).GetAwaiter().GetResult();
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
            else
            {
                return IOUtil.LoadObject<IssueMatchersConfig>(hostPath);
            }
        }
    }
}
