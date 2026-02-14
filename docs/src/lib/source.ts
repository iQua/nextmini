import { docs } from "fumadocs-mdx:collections/server";
import { loader } from "fumadocs-core/source";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";

const sidebarOrder = ["examples", "design", "testing", "config"];

const examplesSidebarOrder = [
	"simple",
	"pytorch",
	"pytorch_python_api",
	"routes",
	"namespace",
	"bare-metal",
	"pytorch-sba",
	"ring_allreduce-sba",
	"localserver-flyio",
	"multicast-flow",
];

const folderOrderByPath: Record<string, string[]> = {
	"": ["index", ...sidebarOrder],
	examples: examplesSidebarOrder,
	config: [
		"index",
		"controller",
		"dataplane",
		"lossless_config",
		"transport",
		"enums",
		"environment",
		"examples",
	],
};

const docsNavOrderPlugin = {
	name: "docs-sidebar-order",
	enforce: "pre" as const,
	transformPageTree: {
		root: (root: any) => {
			root.children = sortSidebarByOrder(root.children);
			return root;
		},
	},
};

function sortSidebarByOrder(
	children: Array<{
		type: string;
		name?: unknown;
		url?: unknown;
		children?: Array<{ type: string; name?: unknown; children?: any }>;
	}>,
	parentPath = "",
) {
	const order = new Map(
		folderOrderByPath[parentPath]?.map((name, index) => [name, index]) ?? [],
	);

	const folderNodes = children.filter((node) => node.type === "folder");

	folderNodes.forEach((folder) => {
		if (Array.isArray(folder.children)) {
			const folderPath =
				typeof folder.name === "string"
					? `${parentPath ? `${parentPath}/` : ""}${folder.name.toLowerCase()}`
					: parentPath;
			folder.children = sortSidebarByOrder(folder.children, folderPath);
		}
	});

	children.sort((a, b) => {
		const firstName = getNodeSortKey(a);
		const secondName = getNodeSortKey(b);
		const first = order.get(firstName);
		const second = order.get(secondName);

		if (first !== undefined || second !== undefined) {
			if (first === undefined) return 1;
			if (second === undefined) return -1;
			if (first === second) return 0;
			return first - second;
		}

		if (firstName && secondName) {
			return firstName.localeCompare(secondName);
		}
		if (!firstName) return 1;
		if (!secondName) return -1;
		return 0;
	});

	const pageNodes = children.filter((node) => node.type !== "folder");
	const sortedFolderNodes = children.filter((node) => node.type === "folder");

	return [...pageNodes, ...sortedFolderNodes];
}

function getNodeSortKey(node: { name?: unknown; url?: unknown }): string {
	if (typeof node.url === "string") {
		const normalized = node.url.replace(/\/+$/, "");
		return normalized.split("/").at(-1) ?? "";
	}

	return typeof node.name === "string" ? node.name.toLowerCase() : "";
}

export const source = loader({
	source: docs.toFumadocsSource(),
	baseUrl: "/docs",
	plugins: [docsNavOrderPlugin, lucideIconsPlugin()],
});
