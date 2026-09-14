import { readFile } from 'node:fs/promises';

export async function readmeExamples() {
  const markdown = await readFile(
    new URL('../README.md', import.meta.url),
    'utf8',
  );
  return [...markdown.matchAll(/```(?:javascript|js)\r?\n([\s\S]*?)```/g)].map(
    (match) => ({
      code: match[1],
      line: markdown.slice(0, match.index).split('\n').length,
    }),
  );
}
