import nextra from "nextra";

const withNextra = nextra({
	theme: "nextra-theme-docs",
	themeConfig: "./theme.config.jsx",
});

const nextConfig = {
	output: "export",
	// basePath: "/~bli/nextmini",
	images: {
		unoptimized: true,
	},
	reactStrictMode: true,
	transpilePackages: ["geist"],
};

export default withNextra(nextConfig);
