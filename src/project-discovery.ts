import { access, readdir, stat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { basename, join } from 'node:path';
import { JSONLStreamParser } from './jsonl-stream-parser.js';

export interface ProjectInfo {
  name: string; // Human-readable project name (from actual cwd)
  actualPath: string; // Real filesystem path (e.g., /Users/user/dev/project/subdir)
  claudePath: string; // Claude projects directory path (e.g., ~/.claude/projects/-Users-user-dev-project-subdir)
  encodedName: string; // Encoded directory name (e.g., -Users-user-dev-project-subdir)
  lastModified: Date;
}

export class ProjectDiscovery {
  private claudeProjectsDir: string;

  constructor() {
    this.claudeProjectsDir = join(homedir(), '.claude', 'projects');
  }

  /**
   * Get all available Claude projects
   */
  async getAllProjects(): Promise<ProjectInfo[]> {
    try {
      const projectDirs = await readdir(this.claudeProjectsDir);
      const projects: ProjectInfo[] = [];
      const parser = new JSONLStreamParser();

      for (const encodedName of projectDirs) {
        try {
          const claudePath = join(this.claudeProjectsDir, encodedName);
          const stats = await stat(claudePath);

          if (stats.isDirectory()) {
            // Extract actual project path from JSONL files
            const actualPath = await parser.extractProjectRoot(claudePath);

            if (actualPath) {
              projects.push({
                name: basename(actualPath), // Use directory name as display name
                actualPath,
                claudePath,
                encodedName,
                lastModified: stats.mtime,
              });
            } else {
              // Fallback: if no cwd found, skip this project
              console.error(`No cwd found in project: ${encodedName}`, {
                file: 'stderr',
              });
            }
          }
        } catch {
          // Skip invalid project directories
        }
      }

      // Sort by last modified (most recent first)
      return projects.sort(
        (a, b) => b.lastModified.getTime() - a.lastModified.getTime()
      );
    } catch (error) {
      throw new Error(
        `Cannot access Claude projects directory: ${(error as Error).message}`
      );
    }
  }

  /**
   * Get current project based on working directory
   */
  async getCurrentProject(): Promise<ProjectInfo | null> {
    const currentDir = process.cwd();
    const encodedPath = this.encodeProjectPath(currentDir);
    const claudePath = join(this.claudeProjectsDir, encodedPath);

    try {
      await access(claudePath);
      const stats = await stat(claudePath);

      return {
        name: basename(currentDir),
        actualPath: currentDir,
        claudePath,
        encodedName: encodedPath,
        lastModified: stats.mtime,
      };
    } catch {
      return null;
    }
  }

  /**
   * Get project by name with fuzzy matching
   */
  async getProject(searchTerm: string): Promise<ProjectInfo | null> {
    const projects = await this.getAllProjects();

    // Exact match on encoded name
    let match = projects.find((p) => p.encodedName === searchTerm);
    if (match) return match;

    // Exact match on project name (basename of actual path)
    match = projects.find((p) => p.name === searchTerm);
    if (match) return match;

    // Exact match on any directory name in the path (prioritize this)
    const exactDirMatches = projects.filter((p) => {
      const pathParts = p.actualPath.split('/');
      return pathParts.some(
        (part) => part.toLowerCase() === searchTerm.toLowerCase()
      );
    });

    if (exactDirMatches.length === 1) {
      return exactDirMatches[0];
    }

    if (exactDirMatches.length > 1) {
      // If multiple exact matches, prefer the shorter path (more specific)
      const shortestPath = exactDirMatches.reduce((shortest, current) =>
        current.actualPath.length < shortest.actualPath.length
          ? current
          : shortest
      );
      return shortestPath;
    }

    // Partial match on actual path (case insensitive) - only if no exact dir matches
    const matches = projects.filter(
      (p) =>
        p.actualPath.toLowerCase().includes(searchTerm.toLowerCase()) ||
        p.name.toLowerCase().includes(searchTerm.toLowerCase())
    );

    if (matches.length === 1) {
      return matches[0];
    }

    if (matches.length > 1) {
      // If multiple partial matches, prefer the shortest path (most specific)
      const shortestPath = matches.reduce((shortest, current) =>
        current.actualPath.length < shortest.actualPath.length
          ? current
          : shortest
      );
      return shortestPath;
    }

    return null;
  }

  /**
   * Get all JSONL files for a project
   */
  async getProjectFiles(projectPath: string): Promise<string[]> {
    try {
      const files = await readdir(projectPath);
      return files
        .filter((file) => file.endsWith('.jsonl'))
        .map((file) => join(projectPath, file));
    } catch (error) {
      throw new Error(`Cannot read project files: ${(error as Error).message}`);
    }
  }

  /**
   * Encode file path to Claude project directory name
   */
  private encodeProjectPath(path: string): string {
    return path.replace(/\//g, '-');
  }
}
