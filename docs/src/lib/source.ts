import { loader } from 'fumadocs-core/source';
import { docs } from 'fumadocs-mdx:collections/server';
import { lucideIconsPlugin } from 'fumadocs-core/source/lucide-icons';

const sidebarOrder = ['examples', 'design', 'testing'];

const docsNavOrderPlugin = {
  name: 'docs-sidebar-order',
  enforce: 'pre' as const,
  transformPageTree: {
    root: (root: any) => {
      root.children = sortSidebarByOrder(root.children);
      return root;
    },
  },
};

function sortSidebarByOrder(children: Array<{ type: string; name?: unknown }>) {
  const order = new Map(sidebarOrder.map((name, index) => [name, index]));
  const pageNodes = children.filter((node) => node.type !== 'folder');
  const folderNodes = children.filter((node) => node.type === 'folder');

  folderNodes.sort((a, b) => {
    const first = order.get(
      typeof a.name === 'string' ? a.name.toLowerCase() : '',
    );
    const second = order.get(
      typeof b.name === 'string' ? b.name.toLowerCase() : '',
    );
    if (first === undefined) return 1;
    if (second === undefined) return -1;
    if (first === second) return 0;
    return first - second;
  });

  return [...pageNodes, ...folderNodes];
}

export const source = loader({
  source: docs.toFumadocsSource(),
  baseUrl: '/docs',
  plugins: [docsNavOrderPlugin, lucideIconsPlugin()],
});
