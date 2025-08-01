import { createReadStream } from 'node:fs';
import { readdir, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import type {
  AssistantConversationEntry,
  ClaudeCommand,
  ConversationEntry,
} from './types.js';

export class JSONLStreamParser {
  private pendingCommands = new Map<string, ClaudeCommand>();
  private maxPendingEntries = 100; // Cleanup threshold

  /**
   * Create resilient streaming parser for single JSONL file
   */
  async *createFileStream(filePath: string): AsyncGenerator<ClaudeCommand> {
    try {
      const fileStream = createReadStream(filePath);
      const rl = createInterface({
        input: fileStream,
        crlfDelay: Number.POSITIVE_INFINITY,
      });

      try {
        yield* this.processLines(rl, filePath);

        // Yield any remaining pending commands at end of file
        yield* this.flushPendingCommands();
      } finally {
        rl.close();
      }
    } catch (error) {
      console.error(
        `Error reading file ${filePath}: ${(error as Error).message}`
      );
    }
  }

  /**
   * Process lines from a readline interface
   */
  private async *processLines(
    rl: ReturnType<typeof createInterface>,
    filePath: string
  ): AsyncGenerator<ClaudeCommand> {
    let lineNumber = 0;
    let entryCount = 0;

    for await (const line of rl) {
      lineNumber++;
      entryCount++;
      const lineStr = line.toString();

      if (!lineStr.trim()) continue;

      // Pre-filter: Skip lines that can't contain bash commands or user commands
      if (!this.lineContainsCommands(lineStr)) {
        continue;
      }

      yield* this.processEntry(lineStr, lineNumber, filePath);

      // Periodic cleanup of old pending commands
      if (entryCount % this.maxPendingEntries === 0) {
        yield* this.cleanupOldPendingCommands();
      }
    }
  }

  /**
   * Check if a line could contain commands or tool results
   */
  private lineContainsCommands(line: string): boolean {
    return (
      line.includes('"Bash"') ||
      line.includes('"tool_result"') ||
      line.includes('<bash-input>')
    );
  }

  /**
   * Process a single entry and handle tool use matching
   */
  private async *processEntry(
    line: string,
    lineNumber: number,
    filePath: string
  ): AsyncGenerator<ClaudeCommand> {
    try {
      const entry: ConversationEntry = JSON.parse(line);

      // Handle bash tool use (store pending)
      const bashCommand = this.extractBashCommand(entry);
      if (bashCommand) {
        const toolUseId = this.extractToolUseId(entry);
        if (toolUseId) {
          this.pendingCommands.set(toolUseId, bashCommand);
        } else {
          // No tool use ID, assume success and yield immediately
          bashCommand.success = true;
          yield bashCommand;
        }
      }

      // Handle tool result (match with pending)
      const toolResult = this.extractToolResult(entry);
      if (toolResult) {
        const command = this.pendingCommands.get(toolResult.tool_use_id);
        if (command) {
          command.success = !toolResult.is_error;
          this.pendingCommands.delete(toolResult.tool_use_id);
          yield command;
        } else {
        }
      }

      // Handle user commands starting with "!" (yield immediately)
      const userCommand = this.extractUserCommand(entry);
      if (userCommand) {
        userCommand.success = true; // User commands are always considered successful
        yield userCommand;
      }
    } catch (error) {
      // Log error to stderr but continue processing
      const errorType =
        error instanceof SyntaxError ? 'JSON syntax' : 'parsing';
      console.error(
        `Error parsing line ${lineNumber} in ${filePath}: ${errorType} error`
      );
    }
  }

  /**
   * Create resilient streaming parser for all files in a project
   */
  async *createProjectStream(
    projectPath: string
  ): AsyncGenerator<ClaudeCommand> {
    try {
      const files = await readdir(projectPath);
      const jsonlFiles = files
        .filter((file) => file.endsWith('.jsonl'))
        .map((file) => join(projectPath, file));

      // Sort files by modification time for chronological order
      const fileStats = await Promise.all(
        jsonlFiles.map(async (file) => ({
          path: file,
          mtime: (await stat(file)).mtime,
        }))
      );

      fileStats.sort((a, b) => a.mtime.getTime() - b.mtime.getTime());

      for (const { path: filePath } of fileStats) {
        yield* this.createFileStream(filePath);
      }
    } catch (error) {
      console.error(
        `Error reading project directory ${projectPath}: ${
          (error as Error).message
        }`
      );
    }
  }

  /**
   * Extract bash command from conversation entry
   */
  extractBashCommand(entry: ConversationEntry): ClaudeCommand | null {
    if (entry.type !== 'assistant' || !entry.message?.content) return null;

    const bashBlock = this.findBashToolBlock(entry.message.content);
    if (!bashBlock) return null;

    return this.createClaudeCommand(bashBlock, entry, 'bash');
  }

  /**
   * Find the first Bash tool use block in content
   */
  private findBashToolBlock(
    content: AssistantConversationEntry['message']['content']
  ) {
    return content.find(
      (block) => block.type === 'tool_use' && block.name === 'Bash'
    );
  }

  /**
   * Extract tool use ID from assistant message containing Bash tool use
   */
  private extractToolUseId(entry: ConversationEntry): string | null {
    if (entry.type !== 'assistant' || !entry.message?.content) return null;

    const bashBlock = this.findBashToolBlock(entry.message.content);
    return bashBlock?.id || null;
  }

  /**
   * Extract tool result from user message
   */
  private extractToolResult(
    entry: ConversationEntry
  ): { tool_use_id: string; is_error: boolean } | null {
    if (entry.type !== 'user' || !entry.message?.content) return null;
    if (typeof entry.message.content === 'string') return null; // Tool results are in array format

    const toolResult = entry.message.content.find(
      (block) => block.type === 'tool_result' && block.tool_use_id
    );

    if (toolResult?.tool_use_id) {
      return {
        tool_use_id: toolResult.tool_use_id,
        is_error: toolResult.is_error ?? false,
      };
    }

    return null;
  }

  /**
   * Flush all pending commands (assume success)
   */
  private *flushPendingCommands(): Generator<ClaudeCommand> {
    for (const command of this.pendingCommands.values()) {
      command.success = true; // Default to success for orphaned commands
      yield command;
    }
    this.pendingCommands.clear();
  }

  /**
   * Clean up old pending commands that haven't been matched
   */
  private *cleanupOldPendingCommands(): Generator<ClaudeCommand> {
    // For now, just flush if we have too many pending
    if (this.pendingCommands.size > this.maxPendingEntries) {
      yield* this.flushPendingCommands();
    }
  }

  /**
   * Create a ClaudeCommand from a tool block and entry
   */
  private createClaudeCommand(
    block: { input?: { command?: string; description?: string } },
    entry: ConversationEntry,
    source: 'bash' | 'user'
  ): ClaudeCommand | null {
    try {
      const command = block.input?.command;
      if (!command) return null;

      return {
        timestamp: entry.timestamp ? new Date(entry.timestamp) : new Date(),
        command: this.normalizeBashCommand(command),
        source,
        description: block.input?.description || undefined,
        projectPath: entry.cwd,
      };
    } catch {
      return null;
    }
  }

  /**
   * Extract user command from <bash-input> tags
   */
  extractUserCommand(entry: ConversationEntry): ClaudeCommand | null {
    if (entry.type !== 'user' || !entry.message?.content) return null;
    if (typeof entry.message.content !== 'string') return null; // User commands are in string format

    const command = this.findUserCommand(entry.message.content);
    if (!command) return null;

    return this.createClaudeCommand({ input: { command } }, entry, 'user');
  }

  /**
   * Find user command from <bash-input> tags
   */
  private findUserCommand(content: string): string | null {
    const bashInputMatch = content.match(/<bash-input>(.*?)<\/bash-input>/);
    if (bashInputMatch) {
      return bashInputMatch[1].trim();
    }

    return null;
  }

  /**
   * Handle multi-line commands (zsh history format)
   */
  normalizeBashCommand(command: string): string {
    return command
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .join('\\n');
  }

  /**
   * Extract project root path from the first few lines of a project's JSONL files
   * Returns the first valid cwd found, or null if none found
   */
  async extractProjectRoot(projectPath: string): Promise<string | null> {
    try {
      const files = await readdir(projectPath);
      const jsonlFiles = files.filter((file) => file.endsWith('.jsonl'));

      // Try each JSONL file until we find a cwd
      for (const file of jsonlFiles) {
        const filePath = join(projectPath, file);
        try {
          const cwd = await this.extractCwdFromFile(filePath);
          if (cwd) return cwd;
        } catch {
          // Skip corrupted files and continue
        }
      }

      return null;
    } catch {
      return null;
    }
  }

  /**
   * Extract cwd from first few lines of a single JSONL file
   */
  private async extractCwdFromFile(
    filePath: string,
    maxLines = 10
  ): Promise<string | null> {
    const fileStream = createReadStream(filePath);
    const rl = createInterface({
      input: fileStream,
      crlfDelay: Number.POSITIVE_INFINITY,
    });

    let lineCount = 0;

    try {
      for await (const line of rl) {
        if (++lineCount > maxLines) break;

        try {
          const entry = JSON.parse(line) as ConversationEntry;
          if (entry.cwd) {
            rl.close();
            fileStream.close();
            return entry.cwd;
          }
        } catch {
          // Skip malformed JSON lines
        }
      }
    } finally {
      rl.close();
      fileStream.close();
    }

    return null;
  }
}

/**
 * Create resilient command stream with error boundaries
 */
export async function* createResilientCommandStream(
  projectPath: string
): AsyncGenerator<ClaudeCommand> {
  const parser = new JSONLStreamParser();

  try {
    yield* parser.createProjectStream(projectPath);
  } catch (error) {
    console.error(`Fatal error in command stream: ${(error as Error).message}`);
  }
}
