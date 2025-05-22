import nextra from "nextra";

/**
 * @type {import('next').NextConfig}
 */
const nextConfig = {
  output: "export",
  // basePath: "/~bli/eecg",
  images: {
    unoptimized: true, // mandatory, otherwise won't export
  },
  // Optional: Change the output directory `out` -> `dist`
  // distDir: "build"
};

const withNextra = nextra({
  // ... other Nextra config options
});

export default withNextra(nextConfig);
