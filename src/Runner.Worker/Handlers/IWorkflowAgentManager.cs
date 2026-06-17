using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GitHub.Runner.Common;
using GitHub.Runner.Worker.Container;

namespace GitHub.Runner.Worker.Handlers
{
    [ServiceLocator(Default = typeof(WorkflowAgentManager))]
    public interface IWorkflowAgentManager : IRunnerService
    {
        Task WriteFileAsync(string podIP, string path, Stream content);

        Task ReadFileAsync(string podIP, string path, Stream outputStream);

        Task<int> ExecuteAsync(
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
            CancellationToken cancellationToken);

        Task SyncWebhookPayloadAsync(IExecutionContext context, string localFilePath, string content);

        void InitializeFileCommand(IExecutionContext context, ContainerInfo container, string hostPath, string contextName);

        Task SyncFileCommandsFromWorkflowPodAsync(IExecutionContext context, ContainerInfo container, string fileCommandDirectory, string fileSuffix, IEnumerable<IFileCommandExtension> commandExtensions);

        Task SyncFileToWorkflowPodAsync(IExecutionContext context, string hostPath);

        Task SyncDirectoryToWorkflowPodAsync(IExecutionContext context, string hostDirectory);
    }
}
