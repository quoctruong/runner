using System.Collections.Generic;
using System.Threading.Tasks;
using GitHub.Runner.Common;
using GitHub.Runner.Worker.Container;

namespace GitHub.Runner.Worker.Handlers
{
    [ServiceLocator(Default = typeof(WorkflowAgentManager))]
    public interface IWorkflowAgentManager : IRunnerService
    {
        Task SyncWebhookPayloadAsync(IExecutionContext context, string localFilePath, string content);
        void InitializeFileCommand(IExecutionContext context, ContainerInfo container, string hostPath, string contextName);
        Task SyncFileCommandsFromWorkflowPodAsync(IExecutionContext context, ContainerInfo container, string fileCommandDirectory, string fileSuffix, IEnumerable<IFileCommandExtension> commandExtensions);
        Task SyncFileToWorkflowPodAsync(IExecutionContext context, string hostPath);
    }
}
