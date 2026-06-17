using System;
using System.IO;
using System.Threading.Tasks;
using GitHub.Runner.Worker;
using GitHub.Runner.Worker.Handlers;
using Moq;
using Xunit;

namespace GitHub.Runner.Common.Tests.Worker.Handlers
{
    public sealed class WorkflowAgentManagerL0
    {
        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Worker")]
        public async Task SyncFileToWorkflowPodAsync_ReturnsEarlyWhenNoSharedVolumeDisabled()
        {
            using (var hostContext = new TestHostContext(this))
            {
                var manager = new WorkflowAgentManager();
                manager.Initialize(hostContext);

                var ec = new Mock<IExecutionContext>();
                await manager.SyncFileToWorkflowPodAsync(ec.Object, "somepath");
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Worker")]
        public async Task SyncDirectoryToWorkflowPodAsync_ReturnsEarlyWhenNoSharedVolumeDisabled()
        {
            using (var hostContext = new TestHostContext(this))
            {
                var manager = new WorkflowAgentManager();
                manager.Initialize(hostContext);

                var ec = new Mock<IExecutionContext>();
                await manager.SyncDirectoryToWorkflowPodAsync(ec.Object, "somedir");
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Worker")]
        public void InitializeFileCommand_ReturnsEarlyWhenNoSharedVolumeDisabled()
        {
            using (var hostContext = new TestHostContext(this))
            {
                var manager = new WorkflowAgentManager();
                manager.Initialize(hostContext);

                var ec = new Mock<IExecutionContext>();
                manager.InitializeFileCommand(ec.Object, null, "somepath", "context");
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Worker")]
        public async Task SyncFileCommandsFromWorkflowPodAsync_ReturnsEarlyWhenNoSharedVolumeDisabled()
        {
            using (var hostContext = new TestHostContext(this))
            {
                var manager = new WorkflowAgentManager();
                manager.Initialize(hostContext);

                var ec = new Mock<IExecutionContext>();
                await manager.SyncFileCommandsFromWorkflowPodAsync(ec.Object, null, "dir", "suffix", null);
            }
        }
    }
}
