using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using GitHub.Runner.Common;
using GitHub.Runner.Worker.Container;

namespace GitHub.Runner.Worker.Handlers
{
    public class WorkflowAgentManager : RunnerService, IWorkflowAgentManager
    {
        public bool IsNoSharedVolumeEnabled()
        {
            return string.Equals(Environment.GetEnvironmentVariable(Constants.Variables.Actions.NoSharedVolume), "true", StringComparison.OrdinalIgnoreCase);
        }

        public async Task SyncWebhookPayloadAsync(IExecutionContext context, string localFilePath, string content)
        {
            if (!IsNoSharedVolumeEnabled()) return;

            var podIP = context.Global.Container?.ContainerIP;
            if (!string.IsNullOrEmpty(podIP))
            {
                try
                {
                    var containerPath = "/github/workflow/event.json";
                    context.Debug($"Syncing event payload to workflow pod: {containerPath}");
                    await WorkflowAgentHelper.WriteFileAsync(podIP, containerPath, content);
                }
                catch (Exception ex)
                {
                    context.Warning($"Failed to upload event payload to workflow pod: {ex.Message}");
                }
            }
        }

        public void InitializeFileCommand(IExecutionContext context, ContainerInfo container, string hostPath, string contextName)
        {
            if (!IsNoSharedVolumeEnabled() || container == null) return;

            var containerPath = container.TranslateToContainerPath(hostPath);
            var podIP = container.ContainerIP;
            if (!string.IsNullOrEmpty(podIP))
            {
                try
                {
                    context.Debug($"Initializing empty file command {contextName} on workflow pod: {containerPath}");
                    WorkflowAgentHelper.WriteFileAsync(podIP, containerPath, string.Empty).GetAwaiter().GetResult();
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
            if (!IsNoSharedVolumeEnabled() || container == null) return;

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
                    var content = await WorkflowAgentHelper.ReadFileAsync(podIP, containerPath);
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
            if (!IsNoSharedVolumeEnabled()) return;

            var podIP = context.Global.Container?.ContainerIP;
            if (!string.IsNullOrEmpty(podIP) && context.Global.Container != null)
            {
                var resolvedScriptPath = context.Global.Container.TranslateToContainerPath(hostPath);
                var scriptContent = File.ReadAllText(hostPath);
                await WorkflowAgentHelper.WriteFileAsync(podIP, resolvedScriptPath, scriptContent);
            }
        }
    }
}
