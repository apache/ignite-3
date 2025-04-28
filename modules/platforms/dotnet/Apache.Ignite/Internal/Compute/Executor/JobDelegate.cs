namespace Apache.Ignite.Internal.Compute.Executor;

using System.Diagnostics.CodeAnalysis;
using Buffers;using Ignite.Compute;

/// <summary>
/// Job execution delegate.
/// </summary>
/// <param name="context">Job execution context.</param>
/// <param name="arg">The input buffer containing a job argument.</param>
/// <param name="responseBuf">The output buffer for storing job execution results.</param>
[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Reviewed.")]
internal delegate void JobDelegate(IJobExecutionContext context, PooledBuffer arg, PooledArrayBuffer responseBuf);
