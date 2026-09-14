import { readFile } from 'node:fs/promises';

export async function readmeExamples() {
  const markdown = await readFile(
    new URL('../README.md', import.meta.url),
    'utf8',
  );
  const counts = new Map();
  return [...markdown.matchAll(/```(?:javascript|js)\r?\n([\s\S]*?)```/g)].map(
    (match) => {
      const before = markdown.slice(0, match.index);
      const headings = [...before.matchAll(/^#{1,6} (.+)$/gm)];
      const heading = headings.at(-1)?.[1] || 'Example';
      const slug = heading
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-|-$/g, '');
      const number = (counts.get(slug) || 0) + 1;
      counts.set(slug, number);
      return {
        code: match[1],
        line: before.split('\n').length,
        id: `${slug}:${number}`,
      };
    },
  );
}
