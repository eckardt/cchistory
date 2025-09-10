import { createResilientCommandStream } from './jsonl-stream-parser.js';
import type { ClaudeCommand } from './types.js';

export interface ProjectInfo {
  name: string;
  actualPath: string;
  claudePath: string;
  encodedName: string;
  lastModified: Date;
}

interface StreamWithBuffer {
  stream: AsyncGenerator<ClaudeCommand>;
  buffer: ClaudeCommand | null; // One command buffered ahead for timestamp comparison
  exhausted: boolean;
}

export class StreamMerger {
  /**
   * Merge multiple project streams chronologically
   *
   * Uses buffering to maintain chronological order without loading all commands
   * into memory. Each stream keeps one command buffered ahead so we can compare
   * timestamps and always yield the earliest command across all projects.
   *
   * This enables efficient streaming (works with pipes like `| head -5`) while
   * ensuring commands from different projects appear in correct chronological order.
   */
  async *mergeProjectStreams(
    projects: ProjectInfo[]
  ): AsyncGenerator<ClaudeCommand> {
    if (projects.length === 0) {
      return;
    }

    if (projects.length === 1) {
      // Single project - just stream it directly
      yield* createResilientCommandStream(projects[0].claudePath);
      return;
    }

    // Create streams for each project
    const streamBuffers: StreamWithBuffer[] = projects.map((project) => ({
      stream: createResilientCommandStream(project.claudePath),
      buffer: null,
      exhausted: false,
    }));

    // Initialize buffers by reading first command from each stream
    await this.initializeBuffers(streamBuffers);

    // Merge streams chronologically
    yield* this.mergeStreamsWithBuffering(streamBuffers);
  }

  /**
   * Merge multiple async generators in chronological order
   * Uses buffering to maintain approximate ordering without loading everything into memory
   */
  async *chronologicalMerge(
    streams: AsyncGenerator<ClaudeCommand>[]
  ): AsyncGenerator<ClaudeCommand> {
    if (streams.length === 0) {
      return;
    }

    if (streams.length === 1) {
      yield* streams[0];
      return;
    }

    const streamBuffers: StreamWithBuffer[] = streams.map((stream) => ({
      stream,
      buffer: null,
      exhausted: false,
    }));

    await this.initializeBuffers(streamBuffers);
    yield* this.mergeStreamsWithBuffering(streamBuffers);
  }

  /**
   * Initialize buffers by reading the first command from each stream
   *
   * This "primes" each stream so we can compare timestamps across all streams
   * and determine which command should be yielded first.
   */
  private async initializeBuffers(
    streamBuffers: StreamWithBuffer[]
  ): Promise<void> {
    for (const streamBuffer of streamBuffers) {
      try {
        const result = await streamBuffer.stream.next();
        if (result.done) {
          streamBuffer.exhausted = true;
        } else {
          streamBuffer.buffer = result.value;
        }
      } catch (error) {
        // Stream error - mark as exhausted
        streamBuffer.exhausted = true;
        console.error(
          `Error initializing stream: ${(error as Error).message}`,
          { file: 'stderr' }
        );
      }
    }
  }

  /**
   * Merge streams using buffering approach
   *
   * Algorithm:
   * 1. Keep one command buffered from each stream
   * 2. Find the stream with the earliest timestamp in its buffer
   * 3. Yield that command and refill its buffer with the next command
   * 4. Repeat until all streams are exhausted
   *
   * This maintains chronological order with O(1) memory per stream.
   */
  private async *mergeStreamsWithBuffering(
    streamBuffers: StreamWithBuffer[]
  ): AsyncGenerator<ClaudeCommand> {
    while (true) {
      const earliestIndex = this.findEarliestCommand(streamBuffers);
      if (earliestIndex === -1) break;

      const command = streamBuffers[earliestIndex].buffer;
      if (!command) continue;

      yield command;
      await this.refillBuffer(streamBuffers[earliestIndex]);
    }
  }

  /**
   * Find the index of the stream with the earliest buffered command
   *
   * Compares timestamps across all non-exhausted streams to maintain
   * chronological order when merging multiple project histories.
   */
  private findEarliestCommand(streamBuffers: StreamWithBuffer[]): number {
    let earliestIndex = -1;
    let earliestTimestamp: Date | null = null;

    for (let i = 0; i < streamBuffers.length; i++) {
      const streamBuffer = streamBuffers[i];
      if (!streamBuffer.exhausted && streamBuffer.buffer) {
        if (
          earliestTimestamp === null ||
          streamBuffer.buffer.timestamp < earliestTimestamp
        ) {
          earliestTimestamp = streamBuffer.buffer.timestamp;
          earliestIndex = i;
        }
      }
    }

    return earliestIndex;
  }

  /**
   * Refill the buffer for a stream after yielding a command
   *
   * Reads the next command from the stream to maintain the buffer invariant:
   * each non-exhausted stream always has one command ready for comparison.
   */
  private async refillBuffer(streamBuffer: StreamWithBuffer): Promise<void> {
    try {
      const result = await streamBuffer.stream.next();
      if (result.done) {
        streamBuffer.exhausted = true;
        streamBuffer.buffer = null;
      } else {
        streamBuffer.buffer = result.value;
      }
    } catch (error) {
      streamBuffer.exhausted = true;
      streamBuffer.buffer = null;
      console.error(`Error reading from stream: ${(error as Error).message}`, {
        file: 'stderr',
      });
    }
  }

  /**
   * Create a sorted buffer for stream merging (used for batch sorting if needed)
   * This is a simpler approach that collects commands in batches
   */
  async *createSortedBuffer(
    streams: AsyncGenerator<ClaudeCommand>[],
    bufferSize = 100
  ): AsyncGenerator<ClaudeCommand> {
    const allStreams = streams.map((stream) => this.streamToArray(stream));
    const streamArrays = await Promise.all(allStreams);

    // Flatten and sort all commands
    const allCommands = streamArrays.flat();
    allCommands.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());

    // Yield in batches
    for (let i = 0; i < allCommands.length; i += bufferSize) {
      const batch = allCommands.slice(i, i + bufferSize);
      for (const command of batch) {
        yield command;
      }
    }
  }

  /**
   * Convert async generator to array (utility function)
   * Used for batch processing when precise ordering is needed
   */
  private async streamToArray(
    stream: AsyncGenerator<ClaudeCommand>
  ): Promise<ClaudeCommand[]> {
    const commands: ClaudeCommand[] = [];
    try {
      for await (const command of stream) {
        commands.push(command);
      }
    } catch (error) {
      console.error(`Error reading stream: ${(error as Error).message}`, {
        file: 'stderr',
      });
    }
    return commands;
  }
}
